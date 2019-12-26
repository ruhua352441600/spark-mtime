package com.mtime.spark.core

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import org.apache.spark.{SparkConf, SparkContext}

object ParseJSON {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ss").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val tx = sc.textFile("aqw_mporder")
    val data = tx.map(line => (gson(line,"theatreId"), gson(line,"orderId"), gson(line,"payAmount")))
                  .foreach(println)

  }
  //解析JSON数据获取元素
  def gson(str: String, ele: String): JsonElement ={
    val json = new JsonParser()
    val obj = json.parse(str).asInstanceOf[JsonObject]
    obj.get(ele)
  }
}
