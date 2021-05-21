package com.pusidun.realtime.dim

import com.pusidun.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 读取商品分类维度到HBase中
bin/maxwell-bootstrap --user maxwell --password pusidunGames##12 --host hadoop101 --database gmall --table base_category3 --client_id maxwell_1
 */
object BaseCategory3App {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BaseCategory3App").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //管理偏移量
    val topic = "ods_base_category3"
    val groupId = "base_category3_group"
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //字符串转换
    val objectDstream: DStream[BaseCategory3] = inputGetOffsetDstream.map {
      record =>{
        val jsonStr: String = record.value()
        val obj: BaseCategory3 = JSON.parseObject(jsonStr, classOf[BaseCategory3])
        obj
      }
    }

    //保存至HBase
    objectDstream.foreachRDD{
      rdd =>{
        rdd.saveToPhoenix(
          "gmall_base_category3",
          Seq("ID", "NAME", "CATEGORY2_ID"),
          new Configuration(),
          Some("hadoop101,hadoop102,hadoop103:2181")
        )

        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
    }

}
