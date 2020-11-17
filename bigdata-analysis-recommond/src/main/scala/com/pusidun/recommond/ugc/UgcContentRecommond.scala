package com.pusidun.recommond.ugc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

//ugc样例类
case class UGCContent(id: Int, content: String, ugcType: Int )
// 定义一个基准推荐对象
case class Recommendation(id: Int, score: Double)
// 定义ugc内容信息提取出的特征向量的ugc相似度列表
case class UGCRecs(id: Int, recs: Seq[Recommendation])


object UgcContentRecommond {

  val UGC_DATA_PATH = this.getClass.getClassLoader.getResource("ugc.txt").getPath

  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建SparkContext
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("UgcContentRecommond")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 加载数据
    val ugcRDD = spark.sparkContext.textFile(UGC_DATA_PATH)
    val ugcDF = ugcRDD.map(
      item => {
        val attr = item.split("\\^")
        UGCContent(attr(0).toInt, attr(3).trim, attr(2).toInt )
      }
    ).toDF()


    // 核心部分： 用TF-IDF从内容信息中提取电影特征向量

    // 创建一个分词器，默认按空格分词
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")

    // 用分词器对原始数据做转换，生成新的一列words
    val wordsData = tokenizer.transform(ugcDF)

    // 引入HashingTF工具，可以把一个词语序列转化成对应的词频
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    // 引入IDF工具，可以得到idf模型
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练idf模型，得到每个词的逆文档频率
    val idfModel = idf.fit(featurizedData)
    // 用模型对原数据进行处理，得到文档中每个词的tf-idf，作为新的特征向量
    val rescaledData = idfModel.transform(featurizedData)
    val ugcFeatures = rescaledData.map(
      row => (row.getAs[Int]("id"), row.getAs[SparseVector]("features").toArray)
    )
      .rdd
      .map(
        x => (x._1, new DoubleMatrix(x._2))
      )
    //ugcFeatures.collect().foreach(println)

    // 对所有电影两两计算它们的相似度，先做笛卡尔积
    val ugcRecs = ugcFeatures.cartesian(ugcFeatures)
      .filter {
        // 把自己跟自己的配对过滤掉
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
        }
      }
      .filter(_._2._2 > 0.8) // 过滤出相似度大于0.8的
      .groupByKey()
      .map {
        case (id, items) => UGCRecs(id, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()


    ugcRecs.write
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "ugcRecs")
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }
  // 求向量余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }
}

