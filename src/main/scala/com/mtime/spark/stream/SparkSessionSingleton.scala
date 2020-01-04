package com.mtime.spark.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
/**
 * Created by Mtime on 2017/5/8.
 */
object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .config("spark.sql.shuffle.partitions", "50")
          .config("spark.sql.warehouse.dir", "file:///C:/Users/Mtime/IdeaProjects/spark-mtime")
        .getOrCreate()
    }
    instance
  }
}
