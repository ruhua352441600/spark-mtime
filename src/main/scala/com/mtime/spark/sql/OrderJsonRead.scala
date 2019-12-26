package com.mtime.spark.sql


import com.google.gson.JsonParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType}

object OrderJsonRead {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                     .builder()
                     .appName("JsonRead")
                     .master("local")
                     .config("spark.sql.warehouse.dir", "file:///C:/Users/Mtime/IdeaProjects/spark-mtime")
                     .getOrCreate()
    //设置日志等级
    spark.sparkContext.setLogLevel("error")

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

    val ticket_order = spark.read.schema(schema1).json("hdfs://192.168.88.124:9000/wanda/TEMP/orders")

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
    val sqlDF = spark.sql("select ticketPayInfo.payValue, " +
      "                                    getArraySum(ticketPayInfo.payValue) payAmount," +
      "                                    ticketPayInfo.payCode," +
      "                                    getPayMode(ticketPayInfo.payCode,ticketPayInfo.payValue) payCode " +
      "                               from orders")
    sqlDF.show()
    spark.stop()
  }

}
