package com.mtime.spark.sql

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.JsonParser
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

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
    val dataRdd = spark.sparkContext.textFile("hdfs://192.168.88.124:9000/wanda/ODS/pos_uturn_ticket_order/dt=*/*")
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

    val data = spark.sql("select explode(str_to_jsonArray(data)) order from orders").distinct()
    //将临时结果以txt的格式保存在hdfs
    //data.coalesce(1)
    //      .write.mode(SaveMode.Overwrite)  //存储模式：覆盖
    //        .format("text")        //存储格式：TXT
    //          .save("hdfs://192.168.88.124:9000/wanda/TEMP/orders")

    val schema1 = new StructType()
  .add("cinemaInnerCode",StringType)
  .add("ticketCode",StringType)
  .add("sellOrderId",StringType)
  .add("webOrderId",StringType)
  .add("refundTicketCode",StringType)
  .add("refundOrderId",StringType)
  .add("isRefund",StringType)
  .add("showCode",StringType)
  .add("isThroughShow",StringType)
  .add("movieCode",StringType)
  .add("oldMovieCode",StringType)
  .add("movieName",StringType)
  .add("hallCode",StringType)
  .add("hallName",StringType)
  .add("hallTypeCode",StringType)
  .add("seatRow",StringType)
  .add("seatColumn",StringType)
  .add("regionId",StringType)
  .add("regionName",StringType)
  .add("movieShowStartTime",StringType)
  .add("movieShowEndTime",StringType)
  .add("ticketTypeCode",StringType)
  .add("ticketTypeName",StringType)
  .add("campaignCode",StringType)
  .add("campaignName",StringType)
  .add("memberId",StringType)
  .add("dealTime",StringType)
  .add("userId",StringType)
  .add("userName",StringType)
  .add("channelCode",StringType)
  .add("channelName",StringType)
  .add("panYing",LongType)
  .add("ticketPrice",LongType)
  .add("moviePrice",LongType)
  .add("ticketPayInfo",
    ArrayType(
      new StructType()
        .add("payCode", StringType)
        .add("payValue", LongType)
        .add("cardNums", StringType)
    )
  )
  .add("serviceChargeValue",LongType)
  .add("serviceChargePayInfo",
    ArrayType(
      new StructType()
        .add("payCode", StringType)
        .add("payValue", LongType)
        .add("cardNums", StringType)
    )
  )
  .add("ticketFee",LongType)
  .add("ticketFeePayInfo",
    ArrayType(
      new StructType()
        .add("payCode", StringType)
        .add("payValue", LongType)
        .add("cardNums", StringType)
    )
  )
  .add("isFill",StringType)
  .add("isReturnServiceCharge",StringType)
  .add("isShowEndReturn",StringType)
  .add("refundTicketFee",LongType)
  .add("refundTicketFeePayInfo",
    ArrayType(
      new StructType()
        .add("payCode", StringType)
        .add("payValue", LongType)
        .add("cardNums", StringType)
    )
  )
  .add("refundTicketFeePayCode",StringType)
  .add("movieShowFeatureType",StringType)

    //将上步骤每行处理好的JSON数据读入
    val ticket_order = spark.read.schema(schema1).json(data.rdd.map(_.toString()))
    ticket_order.createOrReplaceTempView("orders")

    //自定义函数 求array所有元素的和
    spark.udf.register("getArraySum",(list :Seq[Long])=>{
      var sum : Long = 0L
      for (i <- list) {
        sum += i
      }
      sum
    })

    //自定义函数 获取支付金额较大的支付方式
    spark.udf.register("getPayMode",(list1 :Seq[String], list2 :Seq[Long])=>{
      var sum : String = ""
      var payValue : Long = 0L
      var index : Int = 0
      for(i <- 0 to list2.size -1){
        if(list2(i) > payValue){
          payValue = list2(i)
          index = i
        }
      }
      sum=list1(index)
      sum
    })

    //查询SQL
    val sqlDF = spark.sql("""
                                    select cinemaInnerCode,
                                           count(*) cnt
                                      from orders
                                     group by cinemaInnerCode
                                   """)
    sqlDF.show()
    spark.stop()
  }

}
