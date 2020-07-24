package com.pusidun.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark入门案例：统计单词出现的次数
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.指定配置
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    //2.声明上下文
    val sc = new SparkContext(conf)
    //3.逻辑
    val filePath = "./wordCount.txt"
    val result = sc.textFile(filePath).flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _, 1) sortBy(_._2, false)
    //4.打印结果
    result.collect().foreach(println _)
    //关闭资源
    sc.stop()
  }
}
