package com.pusidun.scala.optimize

import org.apache.spark.sql.SparkSession

/**
  * 通过调整分区数量解决数据倾斜案例。
  */
object NewSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("NewSourceTest")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val sourceRdd = sc.textFile("hdfs://bigdata-node02.com:8020//spark-data/new-source-data/p*")
    val kvRDD = sourceRdd.map(_.split("\t")).map(attr =>(attr(0).toLong,attr(1)) )
    //数据倾斜 第八个任务运行时间过长
    kvRDD.groupByKey(12).count()
    //增大分区数量减少数据倾斜
    kvRDD.groupByKey(17).count()
    //减少分区数量减少数据倾斜
    kvRDD.groupByKey(5).count()
    sc.stop()
    spark.stop()
  }
}
