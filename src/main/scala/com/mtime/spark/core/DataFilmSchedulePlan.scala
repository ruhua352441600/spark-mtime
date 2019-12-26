package com.mtime.spark.core

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import org.apache.spark.{SparkConf, SparkContext}

object DataFilmSchedulePlan {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ss").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val schedule_plans = sc.textFile("pos_transfer_sync")
    val data = schedule_plans.map(line => line.split('')(1))
      .filter(s => gson(s,"syncType").toString.replace("\"","") == "Show_Details").foreach(println)

    sc.stop()
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


  //解析JSON数据获取元素
  def gson(str: String, ele: String): JsonElement ={
    val json = new JsonParser()
    val obj = json.parse(str).asInstanceOf[JsonObject]
    obj.get(ele)
  }
}
