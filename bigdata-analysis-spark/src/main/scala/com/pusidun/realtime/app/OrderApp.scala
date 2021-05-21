package com.pusidun.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.pusidun.realtime.bean.BuyInfo
import com.pusidun.realtime.util.{MyESUtil, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 全民消砖块订单汇总
 */
object OrderApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("OrderApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "topic_game_log"
    val groupId = "games_buy_group"


    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      //如果Redis中没有当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //数据二次封装
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        var eventName: String = ""
        try {
          val eventsArray: JSONArray = jsonObject.getJSONArray("events")
          if(eventsArray != null){
            val kvObject: JSONObject = eventsArray.getJSONObject(0)
            if(null != kvObject){
              eventName =  kvObject.getString("en")
            }
          }
        }catch {
          case ex: Exception => {
            println("IO Exception")
          }
        }

        //从json对象中获取时间戳
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间戳转换为日期和小时
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        jsonObject.put("dt", dateStrArr(0))
        jsonObject.put("hr", dateStrArr(1).toInt)
        jsonObject.put("ts", ts)
        jsonObject.put("eventName", eventName)
        jsonObject

      }
    }.filter(_.getString("eventName").equals("diamondShop"))



    jsonObjDStream.foreachRDD {
      rdd => {
        //分区为单位对数据进行处理
        rdd.foreachPartition {
          //遍历每个集合
          jsonObjItr => {
            val list: List[(String, BuyInfo)] = jsonObjItr.map {
              jsonObj => {
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val mid: String = commonJsonObj.getString("mid")
                val uid: String = commonJsonObj.getString("uid")
                val game_name: String = commonJsonObj.getString("game_name")
                val vc: String = commonJsonObj.getString("vc")
                val ar: String = commonJsonObj.getString("ar")
                var buyKind: String = null
                var clientTime: String = null
                try {
                  val eventsArray: JSONArray = jsonObj.getJSONArray("events")
                  if (eventsArray != null) {
                    val firstObject: JSONObject = eventsArray.getJSONObject(0)
                    if (null != firstObject) {
                      val kvObject: JSONObject = firstObject.getJSONObject("kv")
                      buyKind = kvObject.getString("buyKind")
                      clientTime = kvObject.getString("clientTime")
                    }
                  }
                } catch {
                  case ex: Exception => {
                    println("IO Exception")
                  }
                }

                val price: Int = parsePrice(buyKind)
                val priceName: String = parsePriceName(buyKind)
                val info: BuyInfo = BuyInfo(mid, uid, game_name, vc, ar, price, priceName, clientTime, new Date())
                (mid + "_" + new Date(), info)
              }
            }.toList
            //将集合保存都es中
            val dt = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(list, "buy_info_" + dt)
          }
        }
        //提交偏移量到Redis中
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }

    }
    ssc.start()
    ssc.awaitTermination()


  }

  /**
   * 返回每种购买金额
   *
   * @param buyKind 购买种类
   * @return 金额
   */
  def parsePrice(buyKind: String): Int = {
    var result: Int = 0
    if ("0" == buyKind || "2" == buyKind || "12" == buyKind || "16" == buyKind) result = 3
    else if ("1" == buyKind || "25" == buyKind) result = 1
    else if ("3" == buyKind) result = 5
    else if ("4" == buyKind || "8" == buyKind || "13" == buyKind || "15" == buyKind || "17" == buyKind || "21" == buyKind || "26" == buyKind) result = 10
    else if ("5" == buyKind || "14" == buyKind || "19" == buyKind) result = 30
    else if ("6" == buyKind || "9" == buyKind || "23" == buyKind || "27" == buyKind) result = 50
    else if ("7" == buyKind || "11" == buyKind || "24" == buyKind) result = 100
    else if ("10" == buyKind) result = 8
    else if ("18" == buyKind || "20" == buyKind || "22" == buyKind) result = 20
    result
  }


  /**
   * 返回每种购买描述
   *
   * @param buyKind 购买种类
   * @return 每种购买描述
   */
  def parsePriceName(buyKind: String): String = {
    var result = ""
    if ("0" == buyKind) result = "去广告$2.99"
    else if ("1" == buyKind) result = "买100钻石$0.99"
    else if ("2" == buyKind) result = "买300钻石$2.99"
    else if ("3" == buyKind) result = "买580钻石$4.99"
    else if ("4" == buyKind) result = "买1250钻石$9.99"
    else if ("5" == buyKind) result = "买3900钻石$29.99"
    else if ("6" == buyKind) result = "买6750钻石$49.99"
    else if ("7" == buyKind) result = "买15500钻石$99.99"
    else if ("8" == buyKind) result = "一级内购用户礼包$9.99"
    else if ("9" == buyKind) result = "非凡礼包$49.99"
    else if ("10" == buyKind) result = "限时优惠礼包$7.99"
    else if ("11" == buyKind) result = "传说礼包$99.99"
    else if ("12" == buyKind) result = "初学者礼包$2.99"
    else if ("13" == buyKind) result = "移除广告大礼包$9.99"
    else if ("14" == buyKind) result = "超级礼包$29.99"
    else if ("15" == buyKind) result = "每日礼包$9.99"
    else if ("16" == buyKind) result = "一级存钱罐 $2.99"
    else if ("17" == buyKind) result = "二级存钱罐 $9.99"
    else if ("18" == buyKind) result = "三级存钱罐 $19.99"
    else if ("19" == buyKind) result = "四级存钱罐 $29.99"
    else if ("20" == buyKind) result = "至尊礼包$19.99"
    else if ("21" == buyKind) result = "惊喜礼包$9.99"
    else if ("22" == buyKind) result = "超值礼包$19.99"
    else if ("23" == buyKind) result = "钜惠礼包$49.99"
    else if ("24" == buyKind) result = "冠军礼包$99.99"
    else if ("25" == buyKind) result = "麋鹿礼包$0.99"
    else if ("26" == buyKind) result = "圣诞树礼包$9.99"
    else if ("27" == buyKind) result = "圣诞礼包$49.99"
    else if ("106" == buyKind) result = "看广告得钻石"
    result
  }




}
