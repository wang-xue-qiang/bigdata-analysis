package com.pusidun.scala.optimize

import com.pusidun.scala.optimize.SourceData.KeyWordLog
import org.apache.spark.sql.SparkSession

/**
  * 分区不均匀导致数据倾斜
  */
object NewSourceTet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SourceData")
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
