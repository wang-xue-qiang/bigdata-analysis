package com.pusidun.recommond.ugc

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



/**
  * 用户自定义关卡推荐
  */
object UgcOfflineRecommend {
  def main(args: Array[String]): Unit = {

    //创建sparkConf和sparkSession
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UgcOfflineRecommend")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //spark读取mysql关卡
    val ugcContentDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/pusidunpro")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "bricks_ugc")
      .option("user", "root")
      .option("password", "123456")
      .load()
      .createOrReplaceTempView("ugc_content")

    //spark读取mysql关卡用户行为
    val userActionDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/pusidunpro")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "bricks_ugc_action")
      .option("user", "root")
      .option("password", "123456")
      .load()
      .createOrReplaceTempView("ugc_user_action")

    //1) 统计关卡玩人数大于200，平均点赞数排名前200的数据
    val recsDF = spark.sql("select " +
      "ugcId," +
      "count(uid) userTotal, " +
      "sum(praise) praiseTotal, " +
      "sum(praise)/count(uid) avgPraiseTotal " +
      "from ugc_user_action " +
      "group by ugcId " +
      "having count(uid)>200 " +
      "order by avgPraiseTotal desc " +
      "limit 200")

    val connectionProperties = new Properties
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    //recsDF.write.jdbc("jdbc:mysql://localhost:3306/pusidunpro?useUnicode=true&characterEncoding=utf8","ugcHotRecsTop200",connectionProperties)




    import spark.implicits._
    // 加载数据
    val ratingRDD = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "ugcRecs")
      .format("com.mongodb.spark.sql")
      .load()
      .createOrReplaceTempView("ugcRecs")

    // 优秀关卡内容相似度推荐
    val ugcRecs  = spark.sql("select a.id,b.author,b.name,b.enauthor,b.enname,b.play,b.praise,a.recs from ugcRecs a left join ugc_content b  where a.id = b.id  and b.type =3 order by b.play desc")
    ugcRecs.write
      .option("uri", "mongodb://localhost:27017/pusidunGames")
      .option("collection", "ugcContentRecs")
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()







    //2)按照“yyyyMM”格式,统计关卡玩人数大于200，平均点赞数排名前200的数据
/*    val ymRecsDF = spark.sql("select " +
      "date_format(createTime,'yyyy-MM') yearMonth," +
      "ugcId," +
      "count(uid) userTotal, " +
      "sum(praise) praiseTotal, " +
      "sum(praise)/count(uid) avgPraiseTotal  " +
      "from ugc_user_action " +
      "group by date_format(createTime,'yyyy-MM'),ugcId " +
      "having count(uid)>200 " +
      "order by yearMonth desc ,avgPraiseTotal desc " +
      "limit 200")
    ymRecsDF.write
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "ugcYMRecsTop200")
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()*/

    //关闭spark
    spark.stop()
  }
}


