package com.mtime.spark.stream

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.mtime.spark.stream.TicketOrdersSales.{getPay, gson, gsonArray, jsonEleToStr}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object TestKafka {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common")
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val streamContext = new StreamingContext(conf, Seconds(10))
    streamContext.checkpoint("file:///D:\\hadoop-common")
    val group_id = "pos_uturn_ticket_order"
    val topics = Array("pos_uturn_ticket_order").toSet
    val brokerList = "192.168.88.124:9092,192.168.88.125:9092,192.168.88.126:9092"
    val kafkaParams = Map("bootstrap.servers" -> brokerList,
                           "group.id"->group_id,
                           "key.deserializer" -> classOf[StringDeserializer],
                           "value.deserializer" -> classOf[StringDeserializer],
                           "auto.offset.reset" -> "latest")

    val msg = KafkaUtils.createDirectStream[String, String](streamContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    //下面是新增的语句，把DStream保存到MySQL数据库中
    val order = msg.map(_.value())
      .map(line => line.split('')(1))
      .map(data => gsonArray(data))
      .flatMap(s=>s.toIterable)

    //Key
    val orderK = order.map(order =>(
      (jsonEleToStr(gson(order, "cinemaInnerCode")),
        jsonEleToStr(gson(order, "hallCode")),
        jsonEleToStr(gson(order, "movieShowStartTime")).substring(0,10),
        jsonEleToStr(gson(order, "movieShowStartTime")).substring(11,16),
        jsonEleToStr(gson(order, "movieCode")).substring(0,3)
          + jsonEleToStr(gson(order, "movieCode")).substring(4)), order))

    //以组合key与value值
    val orderKV_1 = orderK.map(order => (order._1,1L))
    val orderKV_2 = orderK.map(order => (order._1,getPay(gson(order._2, "ticketPayInfo").toString)))
    orderKV_2.print()
    //
    val orderKV = orderKV_1.join(orderKV_2).reduceByKey((a,b) => (a._1+b._1, a._2+b._2))
    orderKV.print()

    streamContext.start() //spark stream系统启动
    streamContext.awaitTermination()
  }
}
