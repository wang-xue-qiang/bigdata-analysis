package com.pusidun.realtime.app

import com.pusidun.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日活业务
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc = new StreamingContext(conf,Seconds(5))
    var topic:String = "test"
    var groupId:String = "bricks_group"

    val kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    val jsonStr = kafkaDStream.map(_.value())
    jsonStr.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
