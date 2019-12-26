package com.mtime.spark.stream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object TestKafkaHelper {
  def main(args: Array[String]): Unit = {
    //定义状态更新函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common")
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val streamContext = new StreamingContext(conf, Seconds(5))
    streamContext.checkpoint("file:///D:\\hadoop-common")
    val group_id = "TEST"
    val topics = Array("kafka_test").toSet
    val brokerList = "192.168.88.124:9092,192.168.88.125:9092,192.168.88.126:9092"
    val kafkaParams = Map("bootstrap.servers" -> brokerList,
                           "group.id"->group_id,
                           "key.deserializer" -> classOf[StringDeserializer],
                           "value.deserializer" -> classOf[StringDeserializer],
                           "auto.offset.reset" -> "latest")

    val stream = KafkaUtils.createDirectStream[String, String](streamContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val idData = stream.map(record => record.value)
      .map(value => {
        //隐式转换，使用json4s的默认转化器
        implicit val formats: DefaultFormats.type = DefaultFormats
        val json = parse(value)
        //样式类从JSON对象中提取值
        json.extract[log]
      }).map(logData => (logData.id,1))
    //
    case class KafkaMessage(id: String, score: Int)
    case class log(id: String, score: Int)

    val stateDstream = idData.updateStateByKey[Int](updateFunc)
    //下面是新增的语句，把DStream保存到MySQL数据库中
    stateDstream.foreachRDD(rdd => {
      //内部函数
      def func(records: Iterator[(String,Int)]): Unit = {
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try {
          val url = "jdbc:mysql://192.168.88.124:3306/spark"
          val user = "root"
          val password = "root"
          conn = DriverManager.getConnection(url, user, password)
          records.foreach(data => {
            val sql = "replace into wordcount values (?,?,NULL);"
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, data._1.trim)
            stmt.setInt(2, data._2.toInt)
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

    streamContext.start() //spark stream系统启动
    streamContext.awaitTermination()
  }
}
