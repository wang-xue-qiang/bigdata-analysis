package com.pusidun.scala.optimize

import org.apache.spark.sql.SparkSession

/**
  * 缓解数据倾斜 - 调整并行度案例。
  * 实现原理：增加shuffle read task的数量，可以让原本分配给一个task的多个key分配给多个task，从而让每个task处理比原来更少的数据。
  * 方案优点：实现起来比较简单，可以有效缓解和减轻数据倾斜的影响。
  * 方案缺点：只是缓解了数据倾斜而已，没有彻底根除问题，根据实践经验来看，其效果有限。
  * 实践经验：该方案通常无法彻底解决数据倾斜，因为如果出现一些极端情况，比如某个key对应的数据量有100万，那么无论你的task数量增加到多少，都无法处理。
  */
object NewSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("NewSourceTest")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val sourceRdd = sc.textFile("hdfs://node2.com:8020//spark-data/new-source-data/p*")
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
