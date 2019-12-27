package com.mtime.spark.sql

import com.google.gson.JsonParser
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ExplodeOrderArray {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JsonRead")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///C:/Users/Mtime/IdeaProjects/spark-mtime")
      .getOrCreate()
    //设置日志等级
    spark.sparkContext.setLogLevel("error")

    val schemaString = "data sendTime"
    //2 切割 schema成跟数据相同的格式
    val sc =schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true))
    val schema = StructType(sc)
    //把数据读成RDD的格式
    val dataRdd = spark.sparkContext.textFile("hdfs://192.168.88.124:9000/wanda/ODS/pos_uturn_ticket_order/dt=2019-12-19/*")
    //把Rdd切分成行
    val rowRDD = dataRdd.map(_.split("\u0001")).map(attributes => Row(attributes(0), attributes(1).trim))
    val ordersDF = spark.createDataFrame(rowRDD, schema)
    ordersDF.createOrReplaceTempView("orders")

    //自定义函数
    spark.udf.register("str_to_jsonArray",(str :String)=>{
      val json = new JsonParser()
      val jsonElements = json.parse(str).getAsJsonArray()
      var dataList : List[String] = List()
      for (i <- 0 to jsonElements.size()-1) {
        dataList = jsonElements.get(i).toString :: dataList
      }
      dataList
    })

    val data = spark.sql("select explode(str_to_jsonArray(data)) order from orders")
    //将临时结果以txt的格式保存在hdfs
    data.coalesce(1)
          .write.mode(SaveMode.Overwrite)  //存储模式：覆盖
            .format("text")        //存储格式：TXT
              .save("hdfs://192.168.88.124:9000/wanda/TEMP/orders")

    spark.stop()
  }

}
