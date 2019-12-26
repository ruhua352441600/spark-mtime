package com.mtime.spark.stream

import org.apache.spark.{SparkConf, SparkContext}

object TestSparkStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val distData = sc.textFile("data.txt")
    val num = distData.flatMap(_.split(" ")).map(word=>(word, 1)).reduceByKey(_+_)
    num.collect.foreach(println)
    sc.stop()
  }
}
