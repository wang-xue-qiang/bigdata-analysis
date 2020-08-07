package com.pusidun.scala.optimize

import org.apache.spark.{SparkConf, SparkContext}
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
    val conf = new SparkConf()
      .setAppName("NewSourceTest4")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    //准备数据
    val array = new Array[Int](10000)
    for (i <- 0 to 9999) {
      array(i) = new Random().nextInt(10)
    }
    //生成一个rdd
    val rdd = sc.parallelize(array)
    //数据量很大就先取样
    //rdd.sample(false,0.1)
    //所有key加一操作
    val mapRdd = rdd.map((_, 1))
    //没有加随机前缀的结果
    mapRdd.countByKey.foreach(print) //(0,993)(5,998)(1,974)(6,1030)(9,997)(2,1006)(7,967)(3,970)(8,1043)(4,1022)
    //两阶段聚合（局部聚合+全局聚合）处理数据倾斜
    //加随机前缀
    val prifixRdd = mapRdd.map(x => {
      val prifix = new Random().nextInt(10)
      (prifix + "_" + x._1, x._2)
    })
    //加上随机前缀的key进行局部聚合
    val tmpRdd = prifixRdd.reduceByKey(_ + _)
    //去除随机前缀
    val newRdd = tmpRdd.map(x => (x._1.split("_")(1), x._2))
    //最终聚合
    newRdd.reduceByKey(_ + _).foreach(print)//(4,1022)(7,967)(8,1043)(5,998)(6,1030)(9,997)(0,993)(3,970)(2,1006)(1,974)
    sc.stop()
  }
}
