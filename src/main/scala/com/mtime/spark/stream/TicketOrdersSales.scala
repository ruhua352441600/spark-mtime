package com.mtime.spark.stream

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import com.mtime.spark.utils.SparkConstants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object TicketOrdersSales {


  def main(args:Array[String]):Unit={

    val checkpointDir:String=SparkConstants.CHECKPOINTROOT+"/tickets"
    val topicsArray=Array("pos_uturn_ticket_order")


    val ssc1 = (checkPointDir: String) =>{

      val sparkConf=new SparkConf().setAppName("TicketOrdersSale1").setMaster("local")

      val sc=new StreamingContext(sparkConf,Seconds(10))

      sc.checkpoint(checkpointDir)

      val group_id = "pos_uturn_ticket_order1"
      val topics = Array("pos_uturn_ticket_order").toSet
      val brokerList = "192.168.88.124:9092,192.168.88.125:9092,192.168.88.126:9092"
      val kafkaParams = Map("bootstrap.servers" -> brokerList,
                            "group.id"->group_id,
                            "key.deserializer" -> classOf[StringDeserializer],
                            "value.deserializer" -> classOf[StringDeserializer],
                            "auto.offset.reset" -> "latest")

      val msg = KafkaUtils.createDirectStream[String, String](sc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
      //取出msg中每条订单数据[Json格式]
      val order = msg.map(_.value())
                        .map(line => line.split("\u0001")(1))
                           .map(data => gsonArray(data))
                             .flatMap(s=>s.toIterable)
      //Key
      val orderK = order.map(order =>(
        (jsonEleToStr(gson(order, "cinemaInnerCode")),
          jsonEleToStr(gson(order, "hallCode")),
          jsonEleToStr(gson(order, "movieShowStartTime")).substring(0,10),
          jsonEleToStr(gson(order, "movieShowStartTime")).substring(11,16),
          jsonEleToStr(gson(order, "movieCode")).substring(0,3)
            + jsonEleToStr(gson(order, "movieCode")).substring(4)), (1L,getPay(gson(order, "ticketPayInfo").toString))))

      //汇总计算
      val addFunc=(curValues:Seq[(Long, Long)],preValueState:Option[(Long, Long)])=>{
        var curCount_1:Long=0
        var curCount_2:Long=0
        for (curValue <- curValues){
          curCount_1 += curValue._1
          curCount_2 += curValue._2
        }
        val preCount:(Long, Long)=preValueState.getOrElse((0L, 0L))
        Some((curCount_1+preCount._1,curCount_2+preCount._2))
      }

      //更新操作
      val stateDstream = orderK.reduceByKey((a,b) => (a._1+b._1, a._2+b._2)).updateStateByKey(addFunc)

      stateDstream.foreachRDD(rdd => {
        //内部函数
        def func(records: Iterator[((String,String,String,String,String),(Long,Long))]): Unit = {
          var conn: Connection = null
          var stmt: PreparedStatement = null
          try {
            conn = createMysqlConnection()
            records.foreach(data => {
              val sql = "replace into emp values (?,?,?,?,?,?,?);"
              stmt = conn.prepareStatement(sql);
              stmt.setString(1, data._1._1)
              stmt.setString(2, data._1._2)
              stmt.setString(3, data._1._3)
              stmt.setString(4, data._1._4)
              stmt.setString(5, data._1._5)
              stmt.setLong(6, data._2._1)
              stmt.setDouble(7, data._2._2/100)
              stmt.executeUpdate()
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            if (stmt != null) {
              stmt.close()
            }
            if (conn != null) {
              conn.close()
            }
          }
        }
        val repartitionedRDD = rdd.repartition(3)
        repartitionedRDD.foreachPartition(func)
      })

      sc

    }

    val ssc = StreamingContext.getOrCreate(checkpointDir, () => ssc1(checkpointDir))
    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }

  //创建Mysql连接
  def createMysqlConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.88.124:3306/spark", "root", "root")
  }

  //解析JSONArray数据获取元素
  def gsonArray(str: String): List[String] ={
    val json = new JsonParser()
    val jsonElements = json.parse(str).getAsJsonArray()
    var dataList : List[String] = List()
    for (i <- 0 to jsonElements.size()-1) {
      dataList = jsonElements.get(i).toString :: dataList
    }
    dataList
  }

  //获取客户每张票实付总额
  def getPay(str: String): Long ={
    val json = new JsonParser()
    val jsonElements = json.parse(str).getAsJsonArray()
    var dataList : List[String] = List()
    for (i <- 0 to jsonElements.size()-1) {
      dataList = jsonElements.get(i).toString:: dataList
    }
    var pay: Long = 0L
    for (str <- dataList) {
      pay += gson(str,"payValue").toString.toLong
    }
    pay
  }

  //解析JSON数据获取元素
  def gson(str: String, ele: String): JsonElement ={
    val json = new JsonParser()
    val obj = json.parse(str).asInstanceOf[JsonObject]
    obj.get(ele)
  }

  //将字符串类型Json元素转字符串
  def jsonEleToStr(json : JsonElement): String = {
    val str = json.toString.replace("\"","")
    str
  }

}
