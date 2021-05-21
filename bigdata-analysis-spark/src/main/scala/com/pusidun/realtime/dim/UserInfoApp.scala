package com.pusidun.realtime.dim

import java.text.SimpleDateFormat

import com.pusidun.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 用户信息
bin/maxwell-bootstrap --user maxwell --password pusidunGames##12 --host hadoop101 --database gmall --table user_info --client_id maxwell_1
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp")
    val ssc = new StreamingContext(conf, Seconds(5))


    val topic = "ods_user_info"
    val groupId = "user_info_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //字符转格式化
    val userInfoDStream: DStream[UserInfo] = inputGetOffsetDstream.map {
      record => {
        val userInfoJsonStr: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
        //把生日转成年龄
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date = formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val betweenMs = curTs - date.getTime
        val age = betweenMs / 1000L / 60L / 60L / 24L / 365L
        if (age < 20) {
          userInfo.age_group = "20 岁及以下"
        } else if (age > 30) {
          userInfo.age_group = "30 岁以上"
        } else {
          userInfo.age_group = "21 岁到 30 岁"
        }
        if (userInfo.gender == "M") {
          userInfo.gender_name = "男"
        } else {
          userInfo.gender_name = "女"
        }
        userInfo
      }
    }

    userInfoDStream.foreachRDD{
      rdd =>{
        rdd.saveToPhoenix(
          "gmall_user_info",
          Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("hadoop101,hadoop102,hadoop103:2181")
        )
        OffsetManagerUtil.saveOffset(groupId, topic, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
