package com.pusidun.scala.optimize

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 数据优化案例：原始数据处理
  * 数据：id 用户标识 搜索词 时间 url
  * 1 0000067351	阿迪达斯	1595726845721	/k
  * 该job作用处理原始数据：给原始数据添加ID
  */
object SourceData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SourceData")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //val sourceRdd = sc.textFile("./spark.txt")
    val sourceRdd = sc.textFile("hdfs://bigdata-node02.com:8020//spark-data/source-data/spark.txt")
    val df = sourceRdd.map(_.split("\t")).map(attr => KeyWordLog(attr(0).toLong, attr(1), attr(2), attr(3).toLong, attr(3))).toDF()
    df.createOrReplaceTempView("sourceTable")
    //设置12个分区，大部分会落在第八个任务
    val newSourceRdd = spark.sql("SELECT CASE WHEN id < 900000 THEN (8+(CAST( RAND() * 50000 AS BIGINT )) * 12) ELSE id END id,uid,keyWord,time url FROM sourceTable")
    newSourceRdd.rdd.map(_.mkString("\t")).saveAsTextFile("hdfs://bigdata-node02.com:8020//spark-data/new-source-data")
    //newSourceRdd.rdd.map(_.mkString("\t")).saveAsTextFile("./new-source-data")
    sc.stop()
    spark.stop()
  }

  //样例类
  case class KeyWordLog(id: Long, uid: String, keyWord: String, time: Long, url: String)

}
