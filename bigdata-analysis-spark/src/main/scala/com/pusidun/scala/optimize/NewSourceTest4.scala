package com.pusidun.scala.optimize

import org.apache.spark.sql.SparkSession
import scala.util.Random

/**
  * 缓解数据倾斜 - 两阶段聚合（局部聚合+全局聚合）案例。
  * 使用场景：对RDD执行reduceByKey等聚合类shuffle算子  或者 SparkSQL使用group by 语句进行分组聚合。
  * 方案实现原理：将原本相同的key通过附加随机前缀的方式，变成多个不同的key，就可以让原本被一个task处理的数据分散到多个task上去做局部聚合，进而解决单个task处理数据量过多的问题。
  * 接着去除掉随机前缀，再次进行全局聚合，就可以得到最终的结果。
  * 方案优点：对于聚合类的shuffle操作导致的数据倾斜，效果是非常不错的。通常都可以解决掉数据倾斜，或者至少是大幅度缓解数据倾斜，将Spark作业的性能提升数倍以上。
  * 方案缺点：仅仅适用于聚合类的shuffle操作，适用范围相对较窄。如果是join类的shuffle操作，还得用其他的解决方案
  */
object NewSourceTest4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("NewSourceTest3")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    //加载原始数据
    val sourceRdd = sc.textFile("hdfs://bigdata-node02.com:8020//spark-data/new-source-data/p*")
    val kvRdd = sourceRdd.map(_.split("\t")).map(attr => (attr(0).toLong, attr(1))).map(x=>{if(x._1>20000)(20001,x._2)else x})
    kvRdd.groupByKey().count

    //优化
    //局部聚合将key添加范围
    val kvRdd2 = kvRdd.map(x =>{if(x._1==20001)(20001+Random.nextInt(100), x._2) else x})
    kvRdd2.groupByKey().count

    sc.stop()
    spark.stop()
  }
}
