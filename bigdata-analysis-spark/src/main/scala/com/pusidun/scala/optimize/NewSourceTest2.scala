package com.pusidun.scala.optimize

import org.apache.spark.sql.SparkSession

/**
  * 缓解数据倾斜 -自定义Partitioner案例。
  * 适用场景：大量不同的Key被分配到了相同的Task造成该Task数据量过大。
  * 解决方案：使用自定义的Partitioner实现类代替默认的HashPartitioner，尽量将所有不同的Key均匀分配到不同的Task中。
  * 优势：不影响原有的并行度设计。如果改变并行度，后续Stage的并行度也会默认改变，可能会影响后续Stage。
  * 劣势：适用场景有限，只能将不同Key分散开，对于同一Key对应数据集非常大的场景不适用。效果与调整并行度类似，
  * 只能缓解数据倾斜而不能完全消除数据倾斜。而且需要根据数据特点自定义专用的Partitioner，不够灵活。
  *
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
    val sourceRdd = sc.textFile("hdfs://node2.com:8020//spark-data/new-source-data/p*")
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
