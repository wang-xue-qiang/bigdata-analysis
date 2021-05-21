package com.pusidun.realtime.dwd

import com.pusidun.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object OrderDetailApp {
  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_detail";
    val groupId = "order_detail_group"


    //从redis中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //通过偏移量到Kafka中获取数据
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd => {
        //周期性在driver中执行
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    //提取数据
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map {
      record =>{
        val jsonString: String = record.value()
        //订单处理  转换成更方便操作的专用样例类
        val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
      }
    }

    //orderDetailDstream.print(1000)

    //关联维度数据  因为我们这里做了维度退化，所以订单明细直接和商品维度进行关联即可
    //以分区为单位进行处理
    val orderDetailWithSkuDStream: DStream[OrderDetail] = orderDetailDstream.mapPartitions {
      orderDetailItr => {
        val orderDetailList: List[OrderDetail] = orderDetailItr.toList
        //从订单明细中获取所有的商品id
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        //根据商品id到Phoenix中查询出所有的商品
        var sql: String = s"select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from gmall_sku_info where id in('${skuIdList.mkString("','")}')"
        val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val skuInfoMap: Map[String, SkuInfo] = skuJsonObjList.map {
          skuJsonObj => {
            val skuInfo: SkuInfo = JSON.toJavaObject(skuJsonObj, classOf[SkuInfo])
            (skuInfo.id, skuInfo)
          }
        }.toMap

        for (orderDetail <- orderDetailList) {
          val skuInfo: SkuInfo = skuInfoMap.getOrElse(orderDetail.sku_id.toString, null)
          if (skuInfo != null) {
            orderDetail.spu_id = skuInfo.spu_id.toLong
            orderDetail.spu_name = skuInfo.spu_name
            orderDetail.category3_id = skuInfo.category3_id.toLong
            orderDetail.category3_name = skuInfo.category3_name
            orderDetail.tm_id = skuInfo.tm_id.toLong
            orderDetail.tm_name = skuInfo.tm_name
          }
        }

        orderDetailList.toIterator
      }
    }

    orderDetailWithSkuDStream.print(1000)

    //将订单明细数据写回到kafka的DWD层
    orderDetailWithSkuDStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          orderDetail=>{
            MyKafkaSink.send("dwd_order_detail",
              JSON.toJSONString(orderDetail,new SerializeConfig(true)))
          }
        }
        //保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
