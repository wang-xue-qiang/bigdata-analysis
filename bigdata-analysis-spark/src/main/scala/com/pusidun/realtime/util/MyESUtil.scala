package com.pusidun.realtime.util

import java.util

/**
 * es工具类
 */
object MyESUtil {

  //声明Jest客户端工厂
  private var jestFactory: JestClientFactory = null

  //提供获取Jest客户端的方法
  def getJestClient(): JestClient = {
    if (jestFactory == null) {
      //创建Jest客户端工厂对象
      build()
    }
    jestFactory.getObject
  }

  //构造es连接对象
  def build(): Unit = {
    jestFactory = new JestClientFactory
    jestFactory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://hadoop101:9200/")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000).build())
  }

  //向ES中插入单条数据  方式1  将插入文档的数组以json的形式直接传递
  def putIndex1(): Unit = {
    //获取客户端连接
    val jestClient: JestClient = getJestClient()
    //定义执行的source
    var source: String =
      """
        |{
        |  "id":200,
        |  "name":"operation meigong river",
        |  "doubanScore":8.0,
        |  "actorList":[
        |	    {"id":3,"name":"zhang han yu"}
        |	  ]
        |}
      """.stripMargin
    //创建插入类 Index   Builder中的参数表示要插入到索引中的文档，底层会转换Json格式的字符串，所以也可以将文档封装为样例类对象
    val index: Index = new Index.Builder(source)
      .index("movie_index")
      .`type`("movie")
      .id("1")
      .build()
    //通过客户端对象操作ES     execute参数为Action类型，Index是Action接口的实现类
    jestClient.execute(index)
    //关闭连接
    jestClient.close()
  }

  //向ES中插入单条数据  方式2   将向插入的文档封装为一个样例类对象
  def putIndex2(): Unit = {
    val jestClient = getJestClient()

    val actorList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
    val actorMap1: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    actorMap1.put("id", 66)
    actorMap1.put("name", "李若彤")
    actorList.add(actorMap1)

    //封装样例类对象
    val movie: Movie = Movie(300, "天龙八部", 9.0f, actorList)
    //创建Action实现类 ===>Index
    val index: Index = new Index.Builder(movie)
      .index("movie_index")
      .`type`("movie")
      .id("2").build()
    jestClient.execute(index)
    jestClient.close()
  }

  //根据文档的id，从ES中查询出一条记录
  def queryIndexById(): Unit = {
    val jestClient = getJestClient()
    val get: Get = new Get.Builder("movie_index_5", "2").build()
    val res: DocumentResult = jestClient.execute(get)
    println(res.getJsonString)
    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档  方式1
  def queryIndexByCondition1(): Unit = {
    val jestClient = getJestClient()
    var query: String =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "天龙"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "李若彤"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
      """.stripMargin
    //封装Search对象
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5")
      .build()
    val res: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String, Any]])
    //将java的List转换为json的List
    import scala.collection.JavaConverters._
    val resList1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(resList1.mkString("\n"))

    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档  方式2
  def queryIndexByCondition2(): Unit = {
    val jestClient = getJestClient()
    //SearchSourceBuilder用于构建查询的json格式字符串
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder
    val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name", "天龙"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword", "李若彤"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(10)
    searchSourceBuilder.sort("doubanScore", SortOrder.ASC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query: String = searchSourceBuilder.toString
    //println(query)

    val search: Search = new Search.Builder(query).addIndex("movie_index_5").build()
    val res: SearchResult = jestClient.execute(search)
    val resList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String, Any]])

    import scala.collection.JavaConverters._
    val list = resList.asScala.map(_.source).toList
    println(list.mkString("\n"))

    jestClient.close()
  }

  //  向ES中批量插入数据
  def bulkInsert(infoList: List[(String, Any)], indexName: String): Unit = {

    if (infoList != null && infoList.size != 0) {
      //获取客户端
      val jestClient = getJestClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      for ((id, dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }
      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult = jestClient.execute(bulk)
      println("向ES中插入" + bulkResult.getItems.size() + "条数据")
      jestClient.close()
    }
  }

  //测试
  def main(args: Array[String]): Unit = {
    putIndex1()
  }

}


case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Any]]) {}