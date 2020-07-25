package com.pusidun.scala.core

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 过滤并且输出文本中包含字母的，剩下的数字求和。
  */
object CustomerSelfAccumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("accumulator").setMaster("local")
    val sc = new SparkContext(conf)

    val accum = new MyAccumulator
    sc.register(accum, "myAccu")
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2)
      .filter(line => {
        val pattern = """^-?(\d+)"""
        val flag = line.matches(pattern)
        if (!flag) {
          accum.add(line)
        }
        flag
      })
      .map(_.toInt)
      .reduce(_ + _)

    println("数值中数据总和: " + sum)

    println("聚合算子数据："+accum.value)

    sc.stop()
  }
}

/**
  * 自定义累加器具体实现
  */
class MyAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
  // 定义一个累加器的内存结构，用于保存带有字母的字符串。
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc = new MyAccumulator()
    _logArray.synchronized {
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case o: MyAccumulator => _logArray.addAll(o.value)
    }
  }

  override def value: util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }
}