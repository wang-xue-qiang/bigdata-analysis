package com.pusidun.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.pusidun.realtime.util._
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
 * 用户首单分析
 */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = "ods_order_info"
    var groupId = "order_info_group"

    //===================1.从Kafka主题中读取数据====================
    //从Redis获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    //根据偏移量是否存在决定从什么位置开始读取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前批次处理的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对DS的结构进行转换   ConsumerRecord[k,v] ===>value:jsonStr ==>OrderInfo
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        //获取json格式字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        //2020-10-27 14:30:20
        val createTime: String = orderInfo.create_time
        val createTimeArr: Array[String] = createTime.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }

    //===================2.判断是否为首单  ====================
    //方案2  以分区为单位，将整个分区的数据拼接一条SQL进行一次查询
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //当前一个分区中所有的订单的集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中获取下订单的用户
        val userIdList: List[Long] = orderInfoList.map(_.user_id)

        //根据用户集合到Phoenix中查询，看看哪些用户下过单   坑1  字符串拼接
        var sql: String =
          s"select user_id,if_consumed from user_status where user_id in('${userIdList.mkString("','")}')"

        //执行sql从Phoenix获取数据
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //获取消费过的用户id   坑2  大小写
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        for (orderInfo <- orderInfoList) {
          //坑3    类型转换
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }

    /*
    ===================4.同一批次中状态修正  ====================
    应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
    	同一采集周期的同一用户-----按用户分组（groupByKey）
    	最早的订单-----排序，取最早（sortwith）
    	标记为首单-----具体业务代码
    */
    //对待处理的数据进行结构转换orderInfo====>(userId,orderInfo)
    val mapDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map(orderInfo=>(orderInfo.user_id,orderInfo))
    //根据用户id对数据进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = mapDStream.groupByKey()


    val orderInfoRealDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (userId, orderInfoItr) => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //判断在一个采集周期中，用户用户是否下了多个订单
        if (orderInfoList != null && orderInfoList.size > 1) {
          //如果下了多个订单，按照下单时间升序排序
          val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          //取出集合中的第一个元素
          if (sortedOrderInfoList(0).if_first_order == "1") {
            //时间最早的订单首单状态保留为1，其它的都设置为非首单
            for (i <- 1 until sortedOrderInfoList.size) {
              sortedOrderInfoList(i).if_first_order = "0"
            }
          }
          sortedOrderInfoList
        } else {
          orderInfoList
        }
      }
    }

    //===================5.和省份维度表进行关联====================
    /*
    //5.1 方案1：以分区为单位，对订单数据进行处理，和Phoenix中的订单表进行关联
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDStream.mapPartitions {
      orderInfoItr => {
        //转换为List
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中订单对应的省份id
        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
        //根据省份id到Phoenix中查询对应的省份
        var sql: String = s"select id,name,area_code,iso_code from gmall0523_province_info where id in('${provinceIdList.mkString("','")}')"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            //将json对象转换为省份样例类对象
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        //对订单数据进行遍历，用遍历出的省份id ，从provinceInfoMap获取省份对象
        for (orderInfo <- orderInfoList) {
          val proInfo: ProvinceInfo = provinceInfoMap.getOrElse(orderInfo.province_id.toString, null)
          if (proInfo != null) {
            orderInfo.province_name = proInfo.name
            orderInfo.province_area_code = proInfo.area_code
            orderInfo.province_iso_code = proInfo.iso_code
          }
        }
        orderInfoList.toIterator
      }
    }
    orderInfoWithProvinceDStream.print(1000)
    */
    //5.2 方案2  以采集周期为单位对数据进行处理 --->通过SQL将所有的省份查询出来
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDStream.transform {
      rdd => {
        //从Phoenix中查询所有的省份数据
        var sql: String = "select id,name,area_code,iso_code from gmall_province_info"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            //将json对象转换为省份样例类对象
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap
        //定义省份的广播变量
        val bdMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

        rdd.map {
          orderInfo => {
            val proInfo: ProvinceInfo = bdMap.value.getOrElse(orderInfo.province_id.toString, null)
            if (proInfo != null) {
              orderInfo.province_name = proInfo.name
              orderInfo.province_area_code = proInfo.area_code
              orderInfo.province_iso_code = proInfo.iso_code
            }
            orderInfo
          }
        }
      }
    }
    orderInfoWithProvinceDStream.print(1000)


    //===================6.和用户维度表进行关联====================
    //以分区为单位对数据进行处理，每个分区拼接一个sql 到phoenix上查询用户数据
    val orderInfoWithUserInfoDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        //转换为list集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取所有的用户id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //根据id拼接sql语句，到phoenix查询用户
        var sql: String = s"select id,user_level,birthday,gender,age_group,gender_name from gmall_user_info where id in ('${userIdList.mkString("','")}')"
        //当前分区中所有的下单用户
        val userList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userMap: Map[String, UserInfo] = userList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap
        for (orderInfo <- orderInfoList) {
          val userInfoObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfoObj != null) {
            orderInfo.user_age_group = userInfoObj.age_group
            orderInfo.user_gender = userInfoObj.gender_name
          }
        }

        orderInfoList.toIterator
      }
    }
    orderInfoWithUserInfoDStream.print(1000)


    //===================3.维护首单用户状态|||保存订单到ES中  ====================
    //如果当前用户为首单用户（第一次消费），那么我们进行首单标记之后，应该将用户的消费状态保存到Hbase中，等下次这个用户
    //再下单的时候，就不是首单了
    orderInfoWithUserInfoDStream.foreachRDD{
      rdd=>{
        //优化
        rdd.cache()

        //3.1 维护首单用户状态
        //将首单用户过滤出来
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order=="1")
        //注意：在使用saveToPhoenix方法的时候，要求RDD中存放数据的属性个数和Phoenix表中字段数必须要一致
        val userStatusRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }
        userStatusRDD.saveToPhoenix(
          "USER_STATUS",
          Seq("USER_ID","IF_CONSUMED"),
          new Configuration,
          Some("hadoop101,hadoop102,hadoop103:2181")
        )

        //3.2保存订单数据到ES中
        rdd.foreachPartition{
          orderInfoItr=>{
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo=>(orderInfo.id.toString,orderInfo))
            val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "gmall_order_info_" + dateStr)

            //3.4写回到Kafka
            for ((orderInfoId,orderInfo) <- orderInfoList) {
              MyKafkaSink.send("dwd_order_info",JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }

          }
        }


        //3.3提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
