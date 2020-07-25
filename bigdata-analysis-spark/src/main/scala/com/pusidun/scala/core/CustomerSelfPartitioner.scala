package com.pusidun.scala.core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 用户自定义分区
  */
object CustomerSelfPartitioner {

  /**
    * 入口函数
    *
    * @param args 参数
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partitioner").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List("aa.2", "bb.2", "cc.3", "dd.3", "ee.5"))
    data.map((_, 1))
      .partitionBy(new CustomerPartitioner(5))
      .keys
      .saveAsTextFile("hdfs://bigdata-node01.com:8020/partitioner")
    sc.stop()
  }

  /**
    * 自定义分区
    *
    * @param numParts 分区数量
    */
  class CustomerPartitioner(numParts: Int) extends Partitioner {
    //覆盖分区数
    override def numPartitions: Int = numParts

    //覆盖分区号获取函数
    override def getPartition(key: Any): Int = {
      val ckey: String = key.toString
      ckey.substring(ckey.length - 1).toInt % numParts
    }
  }

}
