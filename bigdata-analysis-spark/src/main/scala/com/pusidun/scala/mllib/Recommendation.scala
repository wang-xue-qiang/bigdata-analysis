package com.pusidun.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

/**
  * 协同过滤算法
  */
object Recommendation {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("CollaborativeFiltering")
    val sc = new SparkContext(conf)

    //加载数据
    val path = this.getClass.getClassLoader.getResource("recommend.txt").getPath
    val data = sc.textFile(path)
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //训练模型
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //准备用户数据
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    //生成预测结果
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    //对比结果
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    //生成均方误差
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      println(s"【用户】：${user}  【物品】：${product}  【真实值】：${r1}  【预测值】：${r2}")
      val err = (r1 - r2)
      err * err
    }.mean()
    println("预测的均方误差为 = " + MSE)

    val savePath = "./model/Recommendation"
    //保存模型
    model.save(sc, savePath)
    //加载模型
    val sameModel = MatrixFactorizationModel.load(sc, savePath)

    sc.stop()
  }
}
