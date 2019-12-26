package com.mtime.spark.sql

import java.io.File

import org.apache.spark.sql.SparkSession


object HiveSQLProcess {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("Hive SQL")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql("select * from fds.fds_mx_user_auth_info where register_subject_user = 'C1450000000073412953'").show()
    spark.stop()
  }
}
