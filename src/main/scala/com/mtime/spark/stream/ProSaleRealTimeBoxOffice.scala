package com.mtime.spark.stream

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.io.Resources
import com.mtime.spark.stream.TicketOrdersSales.gsonArray
import com.mtime.spark.utils.MysqlConnectionPool
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * 预售票房 实时计算程序。
 * 由于票房预测需要缓存一段时间的数据到内存中，存在缓存过期问题。
 * 预售的时间较长，可能需要缓存15天甚至30天的数据。为了避免内存溢出，
 * 将实时预售票房和实时票房分开计算，减少单个程序缓存的数据。
 */
object ProSaleRealTimeBoxOffice {
  val log: Logger = Logger.getLogger(ProSaleRealTimeBoxOffice.getClass)
  val appName = "ProSaleBoxOfficeCalc_Streaming"
  val consumer_group_id = "group_ProSaleBoxOfficeCalc"
  val config = loadConfig()
  val url = config.getProperty("db.url")
  val user = config.getProperty("db.username")
  val passwd = config.getProperty("db.password")
  var lastUpateTime = System.currentTimeMillis()

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        """
          |Usage: DirectKafkaWordCount <brokers> <topics>
          |  <zk> is a list of one or more Kafka brokers
          |  <topics> is a list of one or more kafka topics to consume from
          |  <checkpointDirectory> streaming check point Directory
          |  <isLocal> true : false
          |
        """.stripMargin)
      System.exit(1)
    }
    var Array(zkConnect, topics, checkpointDirectory, isLocal) = args
    //
    val ssc = StreamingContext.getOrCreate(
      checkpointDirectory,
      () => functionToCreateContext(zkConnect, topics, checkpointDirectory, isLocal)
    )
    ssc.start()
    ssc.awaitTermination()
  }

  def functionToCreateContext(zkConnect: String,
                              topics: String,
                              checkpointDir: String,
                              isLocal: String): StreamingContext = {
    val topicMap = topics.split(",").map((_, 3)).toMap
    var sparkConf: SparkConf = null
    if (isLocal == "false") {
      sparkConf = new SparkConf()
        .setAppName(appName)
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        .set("spark.kryoserializer.buffer.max.mb", "1024")
    } else if (isLocal == "true") {
      sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("local[6]")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
    } else {
      log.error("****:please Specify run mode.")
      System.exit(1)
    }

    val ssc = new StreamingContext(sparkConf, Seconds(180))
    ssc.checkpoint(checkpointDir)

    // Initial state RDD for mapWithState operation
    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)
    //初始化MySQL中数据
    val initialRDD = ssc.sparkContext.parallelize(initialRDDFromDB(sparkSession)).map(parseRecord(_))
    //读取Kafka中数据
    val msg = KafkaUtils.createStream(ssc, zkConnect, consumer_group_id, topicMap).map(_._2)
    //过滤数据为ticket的数据
    val records = msg.map(line => (line.split("\u0001")(0),line.split("\u0001")(1)))
                       .filter(x=>x._1=="pos_uturn_ticket_order")
                         .map(data => gsonArray(data._2))
                           .flatMap(s=>s.toIterable)
    //过滤数据
    val seatMapsCurrent: DStream[(String, JSONObject)] = records.map(parseRecord(_)).filter(msg => isNeed(msg._2))

    //座位图状态维护
    val updateStateFun: (String, Option[JSONObject], State[JSONObject]) => (String, JSONObject) = updateSeatMapState
    val stateDstream = seatMapsCurrent.mapWithState(
      StateSpec.function(updateStateFun)
        .initialState(initialRDD)
        .timeout(Seconds(108000)) //缓存30个小时的数据
    )

    //座位图信息的状态快照转换成临时表
    val seatMapSnap = stateDstream.stateSnapshots().map(_._2)
    seatMapSnap.foreachRDD { (rdd, time) =>
      val batch_time = getBatchTime(time)
      println("------------batchTime:" + batch_time)
      println("------------seatMapSnap rdd Size:" + rdd.count())
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      val seatmapDF: DataFrame = sparkSession.read.json(rdd.map(_.toJSONString))
      if (seatmapDF.count() < 1) {
        //throw new Exception("seat map Snapshots in null !!!")
        log.warn("seat map Snapshots in null !!!")
      } else {
        calcCurrentBo(sparkSession, batch_time, seatmapDF)
      }
    }

    //将当前批次的座位图信息保存到mysql中
    val seatMapNew = stateDstream.map(_._2)
    seatMapNew.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionOfRecords =>
        val conn = MysqlConnectionPool.getConnection.get
        partitionOfRecords.foreach(saveToDB(_, conn))
        MysqlConnectionPool.closeConnection(conn)
      }
    }
    ssc
  }

  /**
   * 更新座位图的状态
   */
  def updateSeatMapState(moviesKey: String,
                         seatMap: Option[JSONObject],
                         state: State[JSONObject]
                        ): (String, JSONObject) = {
    var newValue: JSONObject = seatMap match {
      case None => {
        val temp = state.get();
        temp;
      }
      case _ => {
        state.update(seatMap.get);
        seatMap.get;
      }
    }
    val output = (moviesKey, newValue)
    output
  }

  /**
   * 计算当前票房
   */
  def calcCurrentBo(sparkSession: SparkSession, batch_time: String, seatmapDF: DataFrame): Unit = {
    //计算实时座位图快照表
    //    seatmapDF.createOrReplaceTempView("tb_bo_real_time")
    seatmapDF.createOrReplaceTempView("tb_bo_real_time_presale")
    seatmapDF.printSchema()

    //将seatmapDF 中的 时光影院ID 转换成专资ID
    val gov_mtime_cinemaId_mapping = impGovMtimeCinemaIdMapping(sparkSession)
    gov_mtime_cinemaId_mapping.createOrReplaceTempView("tb_cinemaid_mapping")
    val seatmapDf2 =  sparkSession.sql(config.getProperty("mapping_cinema_id"))
    seatmapDf2.createOrReplaceTempView("tb_bo_real_time")

    //创建实时汇总表
    val biz_date = getBizDate()
    val movie_summary_realtime_Df = sparkSession.sql(config.getProperty("tb_bo_movie_summary_realtime").replace("#{var_date}", biz_date))
    movie_summary_realtime_Df.cache()
    movie_summary_realtime_Df.createOrReplaceTempView("tb_bo_movie_summary_realtime")

    val single_factor_DF = impMovieFactor(sparkSession)
    single_factor_DF.createOrReplaceTempView("tb_movie_single_factor")

    //导入平均票价表
    val avgPrice_DF = impAvgPriceInfo(sparkSession)
    avgPrice_DF.createOrReplaceTempView("tb_movie_price")

    //计算出最终的各个影片的实时票房的汇总表 tb_movie_bo_pt_params
    val movie_summary_DF = sparkSession.sql(config.getProperty("tb_movie_bo_pt_params").replace("#{dt}", getBizDate()))
    movie_summary_DF.cache()
    movie_summary_DF.createOrReplaceTempView("tb_movie_bo_pt_params")

    //计算影片的全国票房和出票
    val movie_bo_pt_DF = sparkSession.sql(config.getProperty("movie_bo_pt").replace("#{batch_time}", batch_time).replace("#{dt}", getBizDate()))
    movie_bo_pt_DF.createOrReplaceTempView("tb_movie_bo_pt")


    // 将影片的实时票房 持久化到mysql,供数据核对
    val conn = MysqlConnectionPool.getMtimeProConnection.get
    movie_bo_pt_DF.toJSON.collect().foreach(saveMovieBoPt(_, conn))

    //全国总票房和出票系数  --修改逻辑为 汇总单部影片
    //    val totalFactor_DF = impMovieTotalFactor(sparkSession)
    //    totalFactor_DF.createOrReplaceTempView("tb_total_bo_pt_params")

    //计算出 预售每天的：全国总票房和出票
    val preSaleDt = sparkSession.sql("select distinct (dt) as dt from  tb_movie_bo_pt where movie_id<>'' AND movie_id IS NOT NULL").collect();
    preSaleDt.foreach(preSaleBizDate => {
      val total_bo_pt_DF = sparkSession.sql(
        config.getProperty("total_bo_pt")
          .replace("#{batch_time}", batch_time)
          .replace("#{dt}", preSaleBizDate.getString(0))
      )
      total_bo_pt_DF.toJSON.collect().foreach(saveTotalBoPt(_, conn))
    })
    movie_summary_DF.toJSON.collect().foreach(saveMovieBoPtParams(_, conn))
    conn.close()

    //释放缓存
    movie_summary_realtime_Df.unpersist()
    movie_summary_DF.unpersist()
  }

  def saveMovieBoPtBatch(str: String, conn: Connection): Unit = {
    val json: JSONObject = JSON.parseObject(str)
    val sqlStr =
      """ REPLACE INTO tb_movie_bo_pt_batch (
        |  dt,
        |  batch_time,
        |  movie_id,
        |  total_bo,
        |  total_pt
        |)
        |VALUES(?,?,?,?,?)
      """.stripMargin
    try {
      val prep = conn.prepareStatement(sqlStr)
      prep.setString(1, json.getOrDefault("dt", "").toString)
      prep.setString(2, json.getOrDefault("batch_time", "").toString)
      prep.setString(3, json.getOrDefault("movie_id", "").toString)
      prep.setString(4, json.getOrDefault("total_bo", "0").toString)
      prep.setString(5, json.getOrDefault("total_pt", "0").toString)
      prep.execute()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def saveTotalBoPtBatch(str: String, conn: Connection): Unit = {
    val json: JSONObject = JSON.parseObject(str)
    val sqlStr =
      """REPLACE INTO tb_total_bo_pt_batch (dt,batch_time, total_bo, total_pt)
        |VALUES
        |  (?, ?, ?, ?)
      """.stripMargin
    try {
      val prep = conn.prepareStatement(sqlStr)
      prep.setString(1, json.getOrDefault("dt", "").toString)
      prep.setString(2, json.getOrDefault("batch_time", "").toString)
      prep.setString(3, json.getOrDefault("total_bo", "0").toString)
      prep.setString(4, json.getOrDefault("total_pt", "0").toString)
      prep.execute()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def saveMovieBoPt(str: String, conn: Connection): Unit = {
    val json: JSONObject = JSON.parseObject(str)
    val sqlStr =
      """ REPLACE INTO tb_movie_bo_pt_presale (
        |  dt,
        |  movie_id,
        |  total_bo,
        |  total_pt,
        |  mtime_total_seat,
        |  mtime_show_times
        |)
        |VALUES(?,?,?,?,?,?)
      """.stripMargin
    try {
      val movieId = json.getOrDefault("movie_id", "").toString
      if (!movieId.isEmpty) {
        val prep = conn.prepareStatement(sqlStr)
        prep.setString(1, json.getOrDefault("dt", "").toString)
        prep.setString(2, movieId)
        prep.setString(3, json.getOrDefault("total_bo", "0").toString)
        prep.setString(4, json.getOrDefault("total_pt", "0").toString)
        prep.setString(5, json.getOrDefault("mtime_total_seat", "0").toString)
        prep.setString(6, json.getOrDefault("mtime_show_times", "0").toString)
        prep.execute()
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def saveTotalBoPt(str: String, conn: Connection): Unit = {
    val json: JSONObject = JSON.parseObject(str)
    val sqlStr =
      """REPLACE INTO tb_total_bo_pt_presale (dt, total_bo, total_pt)
        |VALUES
        |  (?, ?, ?)
      """.stripMargin
    try {
      val prep = conn.prepareStatement(sqlStr)
      prep.setString(1, json.getOrDefault("dt", "").toString)
      prep.setString(2, json.getOrDefault("total_bo", "0").toString)
      prep.setString(3, json.getOrDefault("total_pt", "0").toString)
      prep.execute()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def saveTotalBoPtParams(str: String, conn: Connection): Unit = {
    val json: JSONObject = JSON.parseObject(str)
    val sqlStr =
      """REPLACE INTO tb_total_bo_pt_params (
        |  TYPE,
        |  dt,
        |  factor_a,
        |  factor_b,
        |  factor_c,
        |  factor_d,
        |  q,
        |  s,
        |  r,
        |  u
        |)
        |VALUES
        |  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """.stripMargin
    try {
      val prep = conn.prepareStatement(sqlStr)
      prep.setString(1, json.getOrDefault("type", "").toString)
      prep.setString(2, json.getOrDefault("dt", "").toString)
      prep.setString(3, json.getOrDefault("factor_a", "0").toString)
      prep.setString(4, json.getOrDefault("factor_b", "0").toString)
      prep.setString(5, json.getOrDefault("factor_c", "0").toString)
      prep.setString(6, json.getOrDefault("factor_d", "0").toString)
      prep.setString(7, json.getOrDefault("q", "0").toString)
      prep.setString(8, json.getOrDefault("s", "0").toString)
      prep.setString(9, json.getOrDefault("r", "0").toString)
      prep.setString(10, json.getOrDefault("u", "0").toString)
      prep.execute()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def saveMovieBoPtParams(str: String, conn: Connection): Unit = {
    val json: JSONObject = JSON.parseObject(str)
    val sqlStr =
      """REPLACE INTO tb_movie_bo_pt_params_presale (
        |  dt,
        |  movie_id,
        |  mtime_persion_times,
        |  mtime_show_times,
        |  mtime_avg_pts,
        |  total_show_times,
        |  factor,
        |  movie_factor,
        |  all_factor,
        |  price,
        |  maoyan_price,
        |  gov_price
        |)
        |VALUES
        |  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """.stripMargin
    try {
      val prep = conn.prepareStatement(sqlStr)
      prep.setString(1, json.getOrDefault("dt", "").toString)
      prep.setString(2, json.getOrDefault("movie_id", "").toString)
      prep.setString(3, json.getOrDefault("mtime_persion_times", "0").toString)
      prep.setString(4, json.getOrDefault("mtime_show_times", "0").toString)
      prep.setString(5, json.getOrDefault("mtime_avg_pts", "0").toString)
      prep.setString(6, json.getOrDefault("total_show_times", "0").toString)
      prep.setString(7, json.getOrDefault("factor", "0").toString)
      prep.setString(8, json.getOrDefault("movie_factor", "0").toString)
      prep.setString(9, json.getOrDefault("all_factor", "0").toString)
      prep.setString(10, json.getOrDefault("price", "0").toString)
      prep.setString(11, json.getOrDefault("maoyan_price", "0").toString)
      prep.setString(12, json.getOrDefault("gov_price", "0").toString)
      prep.execute()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def loadConfig(): Properties = {
    val prop = new Properties()
    var fis = Thread.currentThread().getContextClassLoader.getResourceAsStream("conf/PreSale.properties")
    if (fis == null) {
      fis = Resources.getResource("./conf/PreSale.properties").openStream
    }
    prop.load(fis)
    prop
  }

  def initialRDDFromDB(sparkSession: SparkSession) = {
    var sqlStr = config.getProperty("seatmap_init")
    sqlStr = sqlStr.replace("#{var_date}", getBizDate())
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    sparkSession.read.jdbc(url, sqlStr, prop).toJSON.collect()
  }

  /**
   * 导入当日截止到目前所有的座位图信息
   */
  def impMapSeatInfo(sparkSession: SparkSession): DataFrame = {
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    sparkSession.read.jdbc(url, "tb_bo_real_time", prop)
  }

  /**
   * 导入当前最新的平均票价数据。
   * 最新的平均票价数据 保存在mysql的 tb_movie_price_crawler 表中，每半个小时更新一次
   */
  def impMovieTotalFactor(sparkSession: SparkSession): DataFrame = {
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    sparkSession.read.jdbc(url, "(SELECT * FROM tb_total_bo_pt_params" +
      " WHERE dt=(SELECT MAX(dt) FROM tb_total_bo_pt_params )) " +
      "as tb_total_bo_pt_params", prop)
  }
  /**
   * 导入时光和专资票房的映射关系表:用于将时光的影院ID 转换成 专资的影院ID
   */
  def impGovMtimeCinemaIdMapping(sparkSession: SparkSession): DataFrame = {
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    val sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
    val c = Calendar.getInstance()
    c.add(Calendar.YEAR, -1) //明天
    val dt = sdf.format(c.getTime())
    val sqlStr = "(select * from tb_cinemaid_mapping " +
      "where createtime >'" + dt + "' ) " +
      "as tb_total_bo_pt_params"
    sparkSession.read.jdbc(url, sqlStr, prop)
  }
  /**
   * 导入当前最新的专资-合并影讯 历史数据汇总表
   */
  def impMovieSummaryHis(sparkSession: SparkSession): DataFrame = {
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    sparkSession.read.jdbc(url, "tb_bo_movie_summary_his", prop)
  }

  def impAvgPriceInfo(sparkSession: SparkSession): DataFrame = {
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    sparkSession.read.jdbc(url, "tb_movie_price", prop)
  }

  /**
   * 导入影片系数表
   */
  def impMovieFactor(sparkSession: SparkSession): DataFrame = {
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    sparkSession.read.jdbc(url, "tb_movie_single_factor", prop)
  }

  /**
   * 解析消息信息为json对象
   */
  def parseRecord(msg: String): (String, JSONObject) = {
    val json: JSONObject = JSON.parseObject(msg)
    val cinemaId = json.getOrDefault("gov_cinema_id", json.getOrDefault("cinema_id", "_"))
    val key = json.getOrDefault("biz_date", "_").toString + "_" +
      cinemaId + "_" +
      json.getOrDefault("show_time", "_") + "_" +
      json.getOrDefault("movie_id", "_");
    //将json 中的键type改成键data_type
    json.put("data_type", json.getOrDefault("type", "2"))
    json.remove("type")
    (key, json)
  }

  def saveToDB(json: JSONObject, conn: Connection): Unit = {
    val sqlStr =
      """REPLACE INTO tb_bo_real_time_presale
        |( biz_date, cinema_id, hall_id, show_time, movie_id, show_id, show_update_time
        |, total_seat, no_sale, platform, show_status, hasmap, data_type, is_serial
        |, date_time, appid, authorize, plan_exe_time, exe_time, exe_ret_time
        |)
        |VALUES
        | (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """.stripMargin
    try {
      val prep = conn.prepareStatement(sqlStr)
      prep.setString(1, json.getOrDefault("biz_date", "").toString)
      val dataType = json.getOrDefault("data_type", "").toString
      val cinemaId = if ("3".equals(dataType)) json.getOrDefault("gov_cinema_id", "").toString else json.getOrDefault("cinema_id", "").toString
      prep.setString(2, cinemaId)
      prep.setString(3, json.get("hall_id").toString)
      prep.setString(4, json.get("show_time").toString)
      prep.setString(5, json.get("movie_id").toString)
      prep.setString(6, json.get("show_id").toString)
      prep.setString(7, json.getOrDefault("show_update_time", "").toString)
      prep.setString(8, json.getOrDefault("total_seat", "").toString)
      prep.setString(9, json.getOrDefault("no_sale", "").toString)
      prep.setString(10, json.getOrDefault("platform", "").toString)
      prep.setString(11, json.getOrDefault("show_status", "").toString)
      prep.setString(12, json.getOrDefault("hasmap", "").toString)
      prep.setString(13, dataType)
      prep.setString(14, json.getOrDefault("is_serial", "").toString)
      prep.setString(15, json.getOrDefault("date_time", "").toString)
      prep.setString(16, json.getOrDefault("appid", "").toString)
      prep.setString(17, json.getOrDefault("authorize", "").toString)
      prep.setString(18, json.getOrDefault("plan_exe_time", "").toString)
      prep.setString(19, json.getOrDefault("exe_time", "").toString)
      prep.setString(20, json.getOrDefault("exe_ret_time", "").toString)
      prep.execute()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  /**
   * 获取当前的营业日
   */
  def getBizDate(): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val c = Calendar.getInstance()
    sdf.format(c.getTime())
  }

  /**
   * 获取明天的营业日
   */
  def getBizDateTomorrow(): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val c = Calendar.getInstance()
    c.add(Calendar.DATE, 1) //明天
    sdf.format(c.getTime())
  }

  /**
   * 获取批次时间
   */
  def getBatchTime(batchTime: Time): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    dateFormat.format(new Date(batchTime.milliseconds))
  }

  /**
   * 实时计算过程中：
   *  1. 收到的数据的营业日为当天的数据会保留：参与实时计算
   *  2. 当前时间为21点之后，并且收到数据的营业日是当前日期的明天的数据保留：为零点时的票房计算做数据准备。
   */
  def isNeed(record: JSONObject): Boolean = {
    var isNeed: Boolean = false
    isNeed = record.getOrDefault("biz_date", "_").toString > this.getBizDate()
    isNeed
  }
}
