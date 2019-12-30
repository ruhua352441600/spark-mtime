package com.mtime.spark.stream

import java.io
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.io.Resources
import com.mtime.spark.stream.TicketOrdersSales.gsonArray
import com.mtime.spark.utils.MysqlConnectionPool
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

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
  var lastUpdateTime = System.currentTimeMillis()

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
    val topicMap = topics.split(",").toSet
    var sparkConf: SparkConf = null
    if (isLocal == "false") {
      sparkConf = new SparkConf()
        .setAppName(appName)
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        .set("spark.kryoserializer.buffer.max.mb", "1024")
    } else if (isLocal == "true") {
      sparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("local")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
    } else {
      log.error("****:please Specify run mode.")
      System.exit(1)
    }

    val ssc = new StreamingContext(sparkConf, Seconds(30))
    ssc.checkpoint(checkpointDir)

    // Initial state RDD for mapWithState operation
    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)
    //初始化MySQL中数据
    //val initialRDD = ssc.sparkContext.parallelize(initialRDDFromDB(sparkSession)).map(parseRecord(_))
    val kafkaParams: Map[String, io.Serializable] = Map("bootstrap.servers" -> zkConnect,
      "group.id"->consumer_group_id,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest")
    //读取Kafka中数据
    val msg = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,
                                                            Subscribe[String, String](topicMap, kafkaParams))

    //过滤数据为ticket的数据
    val records = msg.map(_.value())
                      .map(line => (line.split("\u0001")(0),line.split("\u0001")(1)))
                       .filter(x=>x._1=="pos_uturn_ticket_order")
                        .map(data => gsonArray(data._2))
                         .flatMap(s=>s.toIterable)
    //过滤数据
    val ticketMapsCurrent: DStream[(String, JSONObject)] = records.map(parseRecord(_)).filter(msg => isNeed(msg._2))

    ticketMapsCurrent.count().print()
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
  def calcCurrentBo(sparkSession: SparkSession, batch_time: String, seat_map_DF: DataFrame): Unit = {
    //计算实时座位图快照表
    seat_map_DF.createOrReplaceTempView("tb_bo_real_time_preSale")
    seat_map_DF.printSchema()

  }

  /**
   * 加载配置文件
   */
  def loadConfig(): Properties = {
    val prop = new Properties()
    var fis = Thread.currentThread().getContextClassLoader.getResourceAsStream("conf/PreSale.properties")
    if (fis == null) {
      fis = Resources.getResource("./db.properties").openStream
    }
    prop.load(fis)
    prop
  }

  /**
   * 初始化历史数据
   */
  def initialRDDFromDB(sparkSession: SparkSession) = {
    var sqlStr = config.getProperty("seat_map_init")
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
   * 解析消息信息为json对象
   */
  def parseRecord(msg: String): (String, JSONObject) = {
    val json: JSONObject = JSON.parseObject(msg)
    val date_id = parseBizDate(json.getOrDefault("movieShowStartTime","_").toString)
    val show_time = parseShowTime(json.getOrDefault("movieShowStartTime","_").toString)
    val key = date_id +
              "_" +
              show_time +
              "_" +
              json.getOrDefault("cinemaInnerCode","_") +
              "_" +
              json.getOrDefault("movieShowStartTime", "_") +
              "_" +
              json.getOrDefault("movieCode", "_")
    (key, json)
  }

  /**
   * 保存结果到数据库表
   */
  def saveToDB(json: JSONObject, conn: Connection): Unit = {
    val sqlStr =
      """REPLACE INTO tb_bo_real_time_presaletb_bo_real_time
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
   * 解析营业日
   */
  def parseBizDate(str: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val show_time = sdf.parse(str)
    val c = Calendar.getInstance()
    c.setTime(show_time)
    c.add(Calendar.HOUR_OF_DAY, -6)
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    sdf1.format(c.getTime)
  }

  /**
   * 获取放映时间
   */
  def parseShowTime(str: String): String = {
    val show_time = str.split(" ")(1)
    show_time.toString
  }

  /**
   * 获取当前的营业日
   */
  def getBizDate(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
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
    val show_time = record.getOrDefault("movieShowStartTime", "_").toString
    isNeed = this.parseBizDate(show_time) >= "2019-12-01"
    isNeed
  }
}
