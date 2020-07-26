package com.pusidun.scala.optimize

import org.apache.spark.sql.SparkSession

/**
  * 通过自定义分区解决数据倾斜案例。
  * :paste
  *  自定义分区
  */
object NewSourceTest2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("NewSourceTest2")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val sourceRdd = sc.textFile("hdfs://bigdata-node02.com:8020//spark-data/new-source-data/p*")
    val kvRDD = sourceRdd.map(_.split("\t")).map(attr =>(attr(0).toLong,attr(1)) )
    kvRDD.groupByKey(new CustomerPartitioner(12)).count
    kvRDD.groupByKey(new CustomerPartitioner(17)).count
    sc.stop()
    spark.stop()
  }

  //自定义分区
  class CustomerPartitioner(numSize: Int) extends org.apache.spark.Partitioner {
    override def numPartitions: Int = numSize
    override def getPartition(key: Any): Int = {
      val id = key.toString.toInt
      return id % numSize
    }
  }

}
