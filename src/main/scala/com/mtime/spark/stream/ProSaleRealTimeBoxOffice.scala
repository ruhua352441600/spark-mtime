package com.mtime.spark.stream

import java.io
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject, JSONArray}
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

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(checkpointDir)

    // Initial state RDD for mapWithState operation
    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)
    //初始化MySQL中数据
    val initialRDD = ssc.sparkContext.parallelize(initialRDDFromDB(sparkSession)).map(parseRecord(_))
    //initialRDD.foreach(println)
    //构建Kafka消费者启动参数
    val kafkaParams = Map("bootstrap.servers" -> zkConnect,
      "group_id" -> consumer_group_id,
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
    val ticketMaps: DStream[(String, (Long, Long))] = records.map(parseMsg(_)).filter(msg => isNeed(msg._2))
      .map(x => {
                  val operation_type = x._2.getOrDefault("isRefund","-")
                  if (operation_type == "N"){
                     (x._1,(1L,sumJsonArray(x._2.getJSONArray("ticketPayInfo"), "payValue")))
                  }else {
                     (x._1,(-1L,-sumJsonArray(x._2.getJSONArray("ticketPayInfo"), "payValue")))
                  }
                })

    val ticketMapsCurrent = ticketMaps.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))

    //维度状态维护
    val updateStateFun: (String, Option[(Long, Long)], State[(Long, Long)]) => (String, (Long, Long)) = updateSeatMapState

    val stateDstream = ticketMapsCurrent.mapWithState(
      StateSpec.function(updateStateFun)
        .initialState(initialRDD)
        .timeout(Seconds(108000)) //缓存30个小时的数据
    )

    //将当前信息保存到mysql中
    stateDstream.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionOfRecords =>
        val conn = MysqlConnectionPool.getConnection.get
        partitionOfRecords.foreach(saveToDB(_, conn))
        MysqlConnectionPool.closeConnection(conn)
      }
    }

    ssc
  }

  //计算每条支付记录的金额
  def sumJsonArray(list: JSONArray, str: String): Long ={
    var amt: Long = 0L
    for(i <- 0 to list.size()-1){
      amt += list.getJSONObject(i).getOrDefault(str,"0").toString.toLong
    }
    amt
  }

  /**
   * 更新指标
   */
  def updateSeatMapState(moviesKey: String,
                         current: Option[(Long, Long)],
                         state: State[(Long, Long)]
                        ):(String, (Long, Long)) = {
    var newValue: (Long, Long) = current match {
      case None => {
        val temp: (Long, Long)= (state.getOption().getOrElse((0L,0L))._1,state.getOption().getOrElse((0L,0L))._2)
        temp
      }
      case _ => {
        val temp: (Long, Long) = (state.getOption().getOrElse((0L,0L))._1+current.getOrElse((0L,0L))._1,
                                  state.getOption().getOrElse((0L,0L))._2+current.getOrElse((0L,0L))._2)
        state.update(temp)
        temp
      }
    }

    val output = (moviesKey, newValue)
    output
  }

  /**
   * 加载配置文件
   */
  def loadConfig(): Properties = {
    val prop = new Properties()
    var fis = Thread.currentThread().getContextClassLoader.getResourceAsStream("./db.properties")
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
    var sqlStr = config.getProperty("dm_ticket_realtime")
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", passwd)
    sparkSession.read.jdbc(url, sqlStr, prop).toJSON.collect()
  }

  /**
   * 解析消息信息为json对象
   */
  def parseRecord(msg: String): (String, (Long, Long)) = {
    val json: JSONObject = JSON.parseObject(msg)
    val date_id = json.getOrDefault("date_id","-").toString
    val show_time = json.getOrDefault("show_time","-").toString
    val key = date_id +
      "_" +
      show_time +
      "_" +
      json.getOrDefault("cinemaInnerCode","-") +
      "_" +
      json.getOrDefault("movieCode", "-")
    val temp = (json.getOrDefault("ticket_num","0").toString.toLong,
                json.getOrDefault("ticket_amt","0").toString.toLong)
    (key, temp)
  }

  /**
   * 解析消息信息为json对象
   */
  def parseMsg(msg: String): (String, JSONObject) = {
    val json: JSONObject = JSON.parseObject(msg)
    val date_id = parseBizDate(json.getOrDefault("movieShowStartTime","-").toString)
    val show_time = parseShowTime(json.getOrDefault("movieShowStartTime","-").toString)
    val key = date_id +
      "_" +
      show_time +
      "_" +
      json.getOrDefault("cinemaInnerCode","-") +
      "_" +
      json.getOrDefault("movieCode", "-")
    (key, json)
  }

  /**
   * 保存结果到数据库表
   */
  def saveToDB(kv:(String, (Long, Long)), conn: Connection): Unit = {
    val sqlStr =
      """REPLACE INTO dm_ticket_realtime1
        |( date_id, show_time, cinemaInnerCode, movieCode, ticket_num, ticket_amt
        |)
        |VALUES
        | (?, ?, ?, ?, ?, ?)
      """.stripMargin
    val keyArray = kv._1.split("_")
    try {
      val prep = conn.prepareStatement(sqlStr)
      prep.setString(1, keyArray(0).toString)
      prep.setString(2, keyArray(1).toString)
      prep.setString(3, keyArray(2).toString)
      prep.setString(4, keyArray(3).toString)
      prep.setLong(5, kv._2._1.toString.toLong)
      prep.setLong(6, kv._2._2.toString.toLong)
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
