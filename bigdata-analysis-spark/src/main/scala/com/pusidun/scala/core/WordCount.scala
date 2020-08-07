package com.pusidun.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark入门案例：统计单词出现的次数
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.指定配置
    val conf = new SparkConf().setAppName("WordCount")
    //2.声明上下文
    val sc = new SparkContext(conf)
    //3.逻辑
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_+_, 1)
      .sortBy(_._2, false)
      .saveAsTextFile(args(1))
    //4.关闭资源
    sc.stop()
  }
}
