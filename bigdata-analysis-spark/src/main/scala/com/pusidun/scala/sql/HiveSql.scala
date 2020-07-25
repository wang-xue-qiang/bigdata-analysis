package com.pusidun.scala.sql

import org.apache.spark.sql.SparkSession

/**
  * 加载hive
  */
object HiveSql {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
      .builder()
      .appName("HiveSql")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("show tables").show()
    spark.stop()
  }

}
