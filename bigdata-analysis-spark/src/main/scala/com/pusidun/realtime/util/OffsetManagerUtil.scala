package com.pusidun.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 * 维护kafka偏移量
 */
object OffsetManagerUtil {

  /**
   * 从redis中获取偏移量
   * @param topic   主题
   * @param groupId 分组
   * @return
   */
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    //获取客户端连接
    val jedis = MyRedisUtil.getJedisClient()
    //拼接redis key offset:topic:groupId
    var offsetKey = "offset:" + topic + ":" + groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()

    //将java的map转化为scala的map
    import scala.collection.JavaConverters._
    val oMap = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量：" + partition + ":" + offset)
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    oMap
  }


  /**
   * 将偏移量保存到redis
   * @param topic 主题
   * @param groupId 分组
   * @param offsetRanges 偏移量对象
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]):Unit={
    var offsetKey = "offset:" + topic + ":" + groupId
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()
    for (offsetRange <- offsetRanges) {
      //分区
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString,untilOffset.toString)
      println("保存分区" + partitionId + ":" + fromOffset + "----->" + untilOffset)
    }
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }


}
