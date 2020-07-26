package com.pusidun.scala.optimize

import com.pusidun.scala.optimize.NewSourceTest2.CustomerPartitioner
import org.apache.spark.sql.SparkSession

/**
  * Reduce side Join转变为Map sideJoin 案例。
  * 使用场景：大表和小表(几百兆)join操作。
  * 采用广播小RDD全量数据+Map算子来实现
  */
object NewSourceTest3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("NewSourceTest3")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    //加载原始数据
    val sourceRdd = sc.textFile("hdfs://bigdata-node02.com:8020//spark-data/new-source-data/p*")
    val kvRdd = sourceRdd.map(_.split("\t")).map(attr => (attr(0).toLong, attr(1)))

    //大表数据 大量的key相同
    val bigRdd = kvRdd.map(x => {
      if (x._1 < 900001) (900001, x._2) else x
    }).map(x => x._1 + "," + x._2)
    bigRdd.saveAsTextFile("hdfs://bigdata-node02.com:8020//spark-data/join/big-table")

    //小表数据
    val smallRdd = kvRdd.filter(_._1 > 900000).map(x => x._1 + "," + x._2)
    smallRdd.saveAsTextFile("hdfs://bigdata-node02.com:8020//spark-data/join/small-table")

    //测试join
    val bigSource = sc.textFile("hdfs://bigdata-node02.com:8020//spark-data/join/big-table/p*")
    val bigKvRdd = bigSource.map(_.split(",")).map(attr => (attr(0).toLong, attr(1)))
    val smallSource = sc.textFile("hdfs://bigdata-node02.com:8020//spark-data/join/small-table/p*")
    val smallKvRdd = smallSource.map(_.split(",")).map(attr => (attr(0).toLong, attr(1)))
    bigKvRdd.join(smallKvRdd).count()

    //优化
    val broadcastVar = sc.broadcast(smallKvRdd.collectAsMap())
    bigKvRdd.map(x => (x._1, (x._2, broadcastVar.value.getOrElse(x._1, "")))).count()

    sc.stop()
    spark.stop()
  }
}
