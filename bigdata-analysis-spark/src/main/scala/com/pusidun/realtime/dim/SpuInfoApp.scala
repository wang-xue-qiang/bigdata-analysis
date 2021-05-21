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
 * 读取商品spu维度到HBase
bin/maxwell-bootstrap --user maxwell --password pusidunGames##12 --host hadoop101 --database gmall --table spu_info --client_id maxwell_1
 */
object SpuInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SpuInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    //管理偏移量
    val topic = "ods_spu_info"
    val groupId = "spu_info_group"
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val getOffsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //字符串转换
    val spuDStream: DStream[SpuInfo] = getOffsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val info: SpuInfo = JSON.parseObject(jsonStr, classOf[SpuInfo])
        info
      }
    }

    //保存至HBase
    spuDStream.foreachRDD{
      rdd =>{
        rdd.saveToPhoenix(
          "gmall_spu_info",
          Seq("ID","SPU_NAME"),
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
