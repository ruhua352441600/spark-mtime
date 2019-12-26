package com.mtime.spark.utils

import java.io.FileInputStream
import java.util.Properties

object KafkaProperties {

  def getKafkaConsumerProperties():Map[String,Object]={

    val prop=new Properties()
    prop.load(getClass.getClassLoader.getResourceAsStream(SparkConstants.KAFKA_PROPS_PATH))

    val muMap=scala.collection.mutable.Map(("bootstrap.servers",prop.getProperty("kafka.broker.bootstrap.servers")))

    val iter=prop.keySet().iterator()
    while(iter.hasNext){
      val key:String=String.valueOf(iter.next())
      if(key.startsWith(SparkConstants.KAFKA_CONSUMER_KEY)){
        muMap +=key.substring(SparkConstants.KAFKA_CONSUMER_KEY.length)->prop.getProperty(key)
      }
    }

    muMap.toMap
  }

  def main(args: Array[String]): Unit = {

    KafkaProperties.getKafkaConsumerProperties().foreach(println)

  }
}
