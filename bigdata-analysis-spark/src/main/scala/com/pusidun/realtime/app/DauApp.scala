package com.pusidun.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.pusidun.realtime.bean.DauInfo
import com.pusidun.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


/**
 * 统计日活
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "topic_game_log"
    val groupId = "game_log_group"



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
        //从json对象中获取时间戳
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间戳转换为日期和小时
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        jsonObject.put("dt", dateStrArr(0))
        jsonObject.put("hr", dateStrArr(1).toInt)
        jsonObject.put("ts", ts)
        jsonObject
      }
    }



    /**
     * 方法二：以分区为单位对数据进行处理，每一个分区获取一次Redis的连接
     * redis 类型 set    key：  dau：2021-01-23    value: mid    expire   3600*24
     */
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        //每一个分区获取一次Redis的连接
        val jedis = MyRedisUtil.getJedisClient()
        //定义一个集合，用于存放当前分区中第一次登陆的日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        for (jsonObj <- jsonObjItr) {
          //获取日期
          val dt = jsonObj.getString("dt")
          //获取设备id
          val mid = jsonObj.getJSONObject("common").getString("mid")
          val game_name = jsonObj.getJSONObject("common").getString("game_name")
          //拼接操作redis的key
          var dauKey = "dau:" + game_name+dt
          val isFirst = jedis.sadd(dauKey, mid)
          //设置key的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isFirst == 1L) {
            //说明是第一次登录
            filteredList.append(jsonObj)
          }
        }
        jedis.close()
        filteredList.toIterator
      }
    }

    /**
     * 方法三：将数据批量的保存到ES中
     */
    filteredDStream.foreachRDD {
      rdd => {
        //以分区为单位对数据进行处理
        rdd.foreachPartition {
          //遍历每个集合
          jsonObjItr => {
            //数据集合加工处理
            val dauInfoList: List[(String, DauInfo)] = jsonObjItr.map {
              jsonObj => {
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("game_name"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  new Date()
                )
                (dauInfo.mid, dauInfo)
              }
            }.toList
            //将集合保存都es中
            val dt = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "dau_info_" + dt)
          }
        }
        //提交偏移量到Redis中
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
