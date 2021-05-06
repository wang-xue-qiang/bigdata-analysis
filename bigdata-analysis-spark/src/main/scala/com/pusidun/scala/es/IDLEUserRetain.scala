package com.pusidun.scala.es

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

/**
 * idle 留存
 */
object IDLEUserRetain {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IDLEUserRetain")
      .master("local[*]")
      .getOrCreate()


    val options = Map(
      "pushdown" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "3.134.252.54",
      "es.port" -> "9200"
    )

    val query =
      """{
        |  "_source": ["init_time","android_id"],
        |     "query": {
        |     "bool": {
        |       "must":[
        |         { "match": { "app_name": "Android_IdleGame" }},
        |         {"range": {
        |           "init_time": {
        |             "gte": "2020-12-20",
        |             "lte": "2020-12-20"
        |           }
        |         }}
        |       ]
        |     }
        |   }
        |}""".stripMargin

    val df = spark.esDF("bussiness", query, options)
    //df.show()
    df.createOrReplaceTempView("user_action")
    val sql = "select  date(init_time) initDate ,  android_id   from user_action"
    //spark.sql(sql).show()


    //通过spark.read操作读取JSON数据。
    val path = this.getClass.getClassLoader.getResource("idle.csv").getPath
    val userDF = spark.read.csv(path).toDF("uid", "groupId", "init_time")
    userDF.createOrReplaceTempView("user")
    //val userSql = "select date(init_time) initDate ,  uid android_id from user"
    val userSql = "select date(init_time) initDate ,  count(uid) active_users from user where init_time is not null group by initDate order by initDate"
    //spark.sql(userSql).show()


    val userRetainSql =
      "select date(init_time) initDate ,  uid  android_id from user  where  date(init_time)  ='2020-12-19' "
    //spark.sql(userRetainSql).show()
    spark.stop();
    println(genSql(-1))
  }

  /**
   * 留存通用sql
   *
   * @return
   */
  def genSql(step: Int): String = {
    "select " +
      "'"+getDate(step)+"' sysDate," +
      "count(distinct a.android_id) userTotal " +
      "from (select date(init_time) initDate ,  uid  android_id from user  where  date(init_time)  ='"+getDate(step)+"') " +
      "a left join b on a.android_id = b.android_id "
  }

  /**
   * 获取距离当前时间
   *
   * @param step 间隔
   * @return
   */
  def getDate(step: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, step)
    return dateFormat.format(cal.getTime())
  }


}
