package com.mtime.spark.utils

object SparkConstants {
  val KAFKAPROPS:String="C:\\f\\workspace-scala\\demo\\props\\kafka.properties"
  val HDFSROOT:String="hdfs://192.168.88.124:9000/spark"
  val CHECKPOINTROOT:String=HDFSROOT+"/checkpoints"

  val KAFKA_PROPS_PATH:String="kafka.properties"
  val KAFKA_CONSUMER_KEY:String="kafka.newConsumer."
}
