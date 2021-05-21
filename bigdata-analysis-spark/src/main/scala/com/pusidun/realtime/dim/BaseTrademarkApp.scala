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
 * 读取商品品牌维度数据到 HBase
bin/maxwell-bootstrap --user maxwell --password pusidunGames##12 --host hadoop101 --database gmall --table base_trademark --client_id maxwell_1
 */
object BaseTrademarkApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BaseTrademarkApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    val topic = "ods_base_trademark"
    val groupId = "base_trademark_group"

    //维护偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //数据转换
    val objectDStream: DStream[BaseTrademark] = inputGetOffsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val baseTrademark: BaseTrademark = JSON.parseObject(jsonStr, classOf[BaseTrademark])
        baseTrademark
      }
    }
    //保存到HBase
    objectDStream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix(
          "gmall_base_trademark",
          Seq("ID", "TM_NAME"),
          new Configuration,
          Some("hadoop101,hadoop102,hadoop103:2181")
        )
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
