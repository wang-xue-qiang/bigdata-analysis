package com.pusidun.realtime.dim

import com.pusidun.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从Kafka中读取省份数据，写入HBase中
bin/maxwell-bootstrap --user maxwell --password pusidunGames##12 --host hadoop101 --database gmall --table base_province --client_id maxwell_1
 */
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ProvinceInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_province"
    val groupId = "province_info_group"
    //从Redis中读取Kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != kafkaOffsetMap && kafkaOffsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //获取偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //写入到HBase中
    offsetDStream.foreachRDD {
      rdd => {
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            val jsonString: String = record.value()
            //转换为 ProvinceInfo
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])
            provinceInfo
          }
        }
        //写入到HBase中
        provinceInfoRDD.saveToPhoenix(
          "gmall_province_info",
          Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"),
          new Configuration,
          Some("hadoop101,hadoop102,hadoop103:2181")
        )

        //提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }

    }
    ssc.start()
    ssc.awaitTermination()
  }
}
