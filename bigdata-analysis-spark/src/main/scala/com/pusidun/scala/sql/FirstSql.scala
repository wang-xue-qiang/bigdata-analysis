package com.pusidun.scala.sql

import org.apache.spark.sql.SparkSession

/**
  * sparkSQL入门
  */
object FirstSql {
  def main(args: Array[String]) {

    //创建SparkConf()并设置App名称
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    // 通过引入隐式转换可以将RDD的操作添加到 DataFrame上。
    import spark.implicits._

    //通过spark.read操作读取JSON数据。
    val path = this.getClass.getClassLoader.getResource("people.json").getPath
    val df = spark.read.json(path)

    // Displays the content of the DataFrame to
    //show操作类似于Action，将DataFrame直接打印到Console。
    df.show()

    // DSL风格的使用方式中，属性的获取方法
    df.filter($"age" > 21).show()

    // 将DataFrame注册为一张临时表
    df.createOrReplaceTempView("persons")

    // 通过spark.sql方法来运行正常的SQL语句。
    spark.sql("SELECT * FROM persons where age > 21").show()

    // 关闭整个SparkSession。
    spark.stop()
  }
}
