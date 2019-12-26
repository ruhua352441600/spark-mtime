package com.mtime.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("192.168.88.124", 9999)
    val words = lines.flatMap(_.split("\\|"))
    // 计算每一个 batch（批次）中的每一个 word（单词）
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // 在控制台打印出在这个离散流（DStream）中生成的每个 RDD 的前十个元素
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
