package com.mtime.spark.core

import java.text.SimpleDateFormat
import java.util.Calendar

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import org.apache.spark.{SparkConf, SparkContext}

object ParseJSON {
  def main(args: Array[String]): Unit = {

    val time = "2019-12-31 05:00:00"
    println(this.parseBizDate(time))
    println(this.parseShowTime(time))




  }
  //解析JSON数据获取元素
  def gson(str: String, ele: String): JsonElement ={
    val json = new JsonParser()
    val obj = json.parse(str).asInstanceOf[JsonObject]
    obj.get(ele)
  }

  /**
   * 解析营业日
   */
  def parseBizDate(str: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val show_time = sdf.parse(str)
    val c = Calendar.getInstance()
    c.setTime(show_time)
    c.add(Calendar.HOUR_OF_DAY, -6)
    val sdf1= new SimpleDateFormat("yyyy-MM-dd")
    sdf1.format(c.getTime)
  }

  /**
   * 解析营业日
   */
  def parseShowTime(str: String): String = {
    val show_time = str.split(" ")(1)
    show_time.toString
  }
}
