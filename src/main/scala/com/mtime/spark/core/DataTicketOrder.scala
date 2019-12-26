package com.mtime.spark.core

import ParseJSON.gson
import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonParser}
import org.apache.spark.{SparkConf, SparkContext}

object DataTicketOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ss").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val orders = sc.textFile("pos_uturn_ticket_order")
    val jsonData = orders.map(line => line.split('')(1)).map(data => gsonArray(data))
                       .flatMap(s=>s.toIterable)
    val data1 = jsonData.map(order => ((jsonEleToStr(gson(order, "cinemaInnerCode")),
                                    jsonEleToStr(gson(order, "hallCode")),
                                    jsonEleToStr(gson(order, "movieShowStartTime")).substring(0,10),
                                    jsonEleToStr(gson(order, "movieShowStartTime")).substring(11,16),
                                    jsonEleToStr(gson(order, "movieCode")).substring(0,3)
                                       + jsonEleToStr(gson(order, "movieCode")).substring(4))
                                     ,(1,getPay(gson(order, "ticketPayInfo").toString)))
                           ).reduceByKey((a,b) => (a._1+b._1, a._2+b._2))
                               .map(x => (x._1,(x._2._1, x._2._2, 1)))

//    val seqOp = (a: (Int, List[String]), b: String) => a match {
//      case (0, List()) => (1, List(b))
//      case _ => (a._1 + 1, b :: a._2)
//    }
//
//    val combOp = (a: (Int, List[String]), b: (Int, List[String])) => {
//      (a._1 + b._1, a._2 ::: b._2)
//    }
//
//    val data2 = jsonData.map(order => ((jsonEleToStr(gson(order, "cinemaInnerCode")),
//      jsonEleToStr(gson(order, "hallCode")),
//      jsonEleToStr(gson(order, "movieShowStartTime")).substring(0,10),
//      jsonEleToStr(gson(order, "movieShowStartTime")).substring(11,16),
//      jsonEleToStr(gson(order, "movieCode")).substring(0,3)
//        + jsonEleToStr(gson(order, "movieCode")).substring(4)
//    ),jsonEleToStr(gson(order, "showCode")))
//    ).aggregateByKey((0, List[String]()))(seqOp, combOp).map(a => (a._1,a._2._2.distinct.length))

    data1.collect().foreach(println)

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

  def getPay(str: String): Long ={
    val json = new JsonParser()
    val jsonElements = json.parse(str).getAsJsonArray()
    var dataList : List[String] = List()
    for (i <- 0 to jsonElements.size()-1) {
      dataList = jsonElements.get(i).toString:: dataList
    }
    var pay: Long = 0L
    for (str <- dataList) {
      pay += gson(str,"payValue").toString.toLong
    }
    pay
  }


  //解析JSON数据获取元素
  def gson(str: String, ele: String): JsonElement ={
    val json = new JsonParser()
    val obj = json.parse(str).asInstanceOf[JsonObject]
    obj.get(ele)
  }

  def jsonEleToStr(json : JsonElement): String = {
    val str = json.toString.replace("\"","")
    str
  }


}
