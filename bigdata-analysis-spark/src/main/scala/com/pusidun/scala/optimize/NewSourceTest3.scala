package com.pusidun.scala.optimize


import org.apache.spark.sql.SparkSession

/**
  * 缓解数据倾斜 - Reduce side Join转变为Map side Join案例。
  * 方案适用场景：在对RDD使用join类操作，或者是在Spark SQL中使用join语句时，而且join操作中的一个RDD或表的数据量比较小（比如几百M），
  * 比较适用此方案。
  * 方案实现原理：普通的join是会走shuffle过程的，而一旦shuffle，就相当于会将相同key的数据拉取到一个shuffle read task中再进行join，
  * 此时就是reduce join。但是如果一个RDD是比较小的，则可以采用广播小RDD全量数据+map算子来实现与join同样的效果，也就是map join，
  * 此时就不会发生shuffle操作，也就不会发生数据倾斜。
  * 方案优点：对join操作导致的数据倾斜，效果非常好，因为根本就不会发生shuffle，也就根本不会发生数据倾斜。
  * 方案缺点：适用场景较少，因为这个方案只适用于一个大表和一个小表的情况。
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
