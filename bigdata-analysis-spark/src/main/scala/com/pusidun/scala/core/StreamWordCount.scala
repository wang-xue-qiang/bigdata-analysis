package com.pusidun.scala.core

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
  * 实时统计wordCount
  * linux安装nc插件
  * yum install -y nc
  * nc -lk 6060
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val hostname:String = "bigdata-cloudera-master.com"
    val port:Int = 6060
    val result = ssc.socketTextStream(hostname, port).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
