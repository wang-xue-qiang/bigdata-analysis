package com.pusidun.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

/**
  * FPGGroup频繁项集挖掘算法
  */
object FPGrowth extends App {

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  //创建SparkContext
  val conf = new SparkConf().setMaster("local").setAppName("FPGrowth")
  val sc = new SparkContext(conf)

  //加载数据样本
  val path = this.getClass.getClassLoader.getResource("fpgrowth.txt").getPath

  //创建交易样本
  val transactions = sc.textFile(path).map(_.split(" "))

  println(s"大乐透历史开奖期数： ${transactions.count()}")

  //最小支持度（0，1）
  val minSupport = 0.001
  //计算的并行度
  val numPartition = 2

  //训练模型
  val model = new FPGrowth()
    .setMinSupport(minSupport)
    .setNumPartitions(numPartition)
    .run(transactions)

  //打印模型结果
  println(s"红球频繁项集数量： ${model.freqItemsets.count()}")

  model.freqItemsets.collect().foreach { itemset =>{
    if (itemset.freq == 2) {
      println(itemset.items.mkString("[", ",", "]") + "\t" + itemset.freq)
    }
  }

  }

  sc.stop()

}