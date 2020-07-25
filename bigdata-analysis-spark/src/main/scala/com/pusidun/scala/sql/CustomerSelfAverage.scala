package com.pusidun.scala.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * 自定义聚合函数求平均值
  */
object CustomerSelfAverage {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CustomerSelfAverage")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val path = this.getClass.getClassLoader.getResource("employees.json").getPath
    val ds = spark.read.json(path).as[Employee]
    ds.show()

    val avgSalary = new MyAverage().toColumn.name("average_salary")
    val result = ds.select(avgSalary)
    result.show()

    spark.stop()
  }

}

//员工信息样例类
case class Employee(name: String, salary: Long)
//请平均样例类
case class Average(var sum: Long, var count: Long)

//自定义聚合函数
class MyAverage extends Aggregator[Employee, Average, Double] {
  // 定义一个数据结构，保存工资总数和工资总个数，初始都为0
  override def zero: Average = Average(0,0)
  // 聚合相同executor分片中的结果
  override def reduce(b: Average, a: Employee): Average = {
    b.sum += a.salary
    b.count += 1
    b
  }
  // 聚合不同execute的结果
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count  += b2.count
    b1
  }
  // 计算输出
  override def finish(reduction: Average): Double = reduction.sum/reduction.count
  // 设定中间值类型的编码器，要转换成case类其中Encoders.product是进行scala元组和case类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product
  // 设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}