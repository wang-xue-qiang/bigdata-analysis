package com.pusidun.scala.core

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * wordCount累加上一段时间出现的单词
  * nc -lk 6060
  */
object StreamWordCountWithState {
  def main(args: Array[String]): Unit = {

    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果。是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./StreamWordCountWithState")
    val hostname:String = "bigdata-cloudera-master.com"
    val port:Int = 6060
    val result = ssc.socketTextStream(hostname, port)
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey[Int](addFunc)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
