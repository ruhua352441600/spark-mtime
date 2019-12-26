package com.mtime.spark.stream

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.mtime.spark.utils.{KafkaProperties, SparkConstants}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object WordCountMain {

  def main(args:Array[String]):Unit={

    val checkpointDir:String=SparkConstants.CHECKPOINTROOT+"/kafka_test"

    val topicsArray=Array("kafka_test")


    val ssc=StreamingContext.getOrCreate(checkpointDir,()=>{

      val sparkConf=new SparkConf().setAppName("wordCount").setMaster("local[2]")

      val sc=new StreamingContext(sparkConf,Seconds(10))

      sc.checkpoint(checkpointDir)

      val msg=KafkaUtils.createDirectStream(sc,PreferConsistent,Subscribe[String,String](topicsArray,KafkaProperties.getKafkaConsumerProperties()))

      val idData = msg.map(_.value()).flatMap(_.split(",")).map(x=>(x,1L))
      //
      val addFunc=(curValue:Seq[Long],preValueState:Option[Long])=>{
          val curCount:Long=curValue.sum
          val preCount:Long=preValueState.getOrElse(0)
          Some(curCount+preCount)
      }

      val stateDstream = idData.updateStateByKey(addFunc)

      stateDstream.foreachRDD(rdd => {
        //内部函数
        def func(records: Iterator[(String,Long)]): Unit = {
          var conn: Connection = null
          var stmt: PreparedStatement = null
          try {
            conn = createMysqlConnection()
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

      sc

    })

    ssc.start()
    ssc.awaitTermination()
  }

  def createMysqlConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.88.124:3306/spark", "root", "root")
  }
}
