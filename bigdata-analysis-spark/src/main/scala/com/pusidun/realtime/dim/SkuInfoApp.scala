package com.pusidun.realtime.dim

import com.pusidun.realtime.util.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 读取商品维度数据，并关联品牌、分类、Spu，保存到 HBase
 * bin/maxwell-bootstrap --user maxwell --password pusidunGames##12 --host hadoop101 --database gmall --table sku_info --client_id maxwell_1
 */
object SkuInfoApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SkuInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    //从kafka获取信息
    val topic = "ods_sku_info";
    val groupId = "dim_sku_info_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val getOffsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    //转换
    val objectDStream: DStream[SkuInfo] = getOffsetDStream.map(
      rdd => {
        val jsonStr: String = rdd.value()
        val skuInfo: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
        skuInfo
      }
    )

    //商品和品牌,分类.spu.进行关联
    val skuInfoDStream: DStream[SkuInfo] = objectDStream.transform {
      rdd => {
        if (rdd.count() > 0) {

          //品牌
          val tmSql = "select id,tm_name from gmall_base_trademark"
          val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
          val tmMap: Map[String, JSONObject] = tmList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

          //category3
          val category3Sql = "select id ,name from gmall_base_category3"
          val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
          val category3Map: Map[String, JSONObject] = category3List.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

          //spu
          val spuSql = "select id ,spu_name from gmall_spu_info"
          val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
          val spuMap: Map[String, JSONObject] = spuList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

          //汇总到一个list 广播这个map
          val dimList: List[Map[String, JSONObject]] = List[Map[String, JSONObject]](category3Map, tmMap, spuMap)
          val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)

          val skuInfoRDD: RDD[SkuInfo] = rdd.mapPartitions {
            skuInfoItr => {
              val dimList: List[Map[String, JSONObject]] = dimBC.value
              val category3Map: Map[String, JSONObject] = dimList(0)
              val tmMap: Map[String, JSONObject] = dimList(1)
              val spuMap: Map[String, JSONObject] = dimList(2)

              val skuInfoList: List[SkuInfo] = skuInfoItr.toList
              for (skuInfo <- skuInfoList) {

                val category3JsonObj: JSONObject = category3Map.getOrElse(skuInfo.category3_id, null)
                if (category3JsonObj != null) {
                  skuInfo.category3_name = category3JsonObj.getString("NAME")
                }

                val tmJsonObj: JSONObject = tmMap.getOrElse(skuInfo.tm_id, null)
                if (tmJsonObj != null) {
                  skuInfo.tm_name = tmJsonObj.getString("TM_NAME")
                }

                val spuJsonObj: JSONObject = spuMap.getOrElse(skuInfo.spu_id, null)
                if (spuJsonObj != null) {
                  skuInfo.spu_name = spuJsonObj.getString("SPU_NAME")
                }

              }
              skuInfoList.toIterator
            }
          }

          skuInfoRDD
        } else {
          rdd
        }
      }
    }

    //保存到HBase
    skuInfoDStream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix(
          "GMALL_SKU_INFO",
          Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
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
