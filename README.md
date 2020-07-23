## bigdata-analysis
  大数据收集，实时分析，离线分析经典案例

## bigdata-analysis-collect
  模拟数据两大类一类生成文件、另一类生成kafka数据。
    * 模拟nginx请求日志；
    * 模拟电商平台用户信息注册；
    * 模拟电商平台用户搜索日志；
    * 模拟电商平台用户点击品牌；
    * 模拟电商平台用户启动信息；
    
## bigdata-analysis-flink
  批处理：
  1.电商平台用户信息分析写入HBase;
  2.统计电商平台每个用户常用搜索词，（TF-IDF）算法使用；
  实时处理案例：
  1.实时统计每小时电商平台中用户喜爱的品牌；
  2.实时统计网站每小时请求排行的前N名；

## bigdata-analysis-hadoop
  离线处理：
  1.wordcount案例；
  2.topn案例；
  3.多job串行案例；
  4.HiveJDBC查询案例；
  5.ImpalaJDBC查询案例；

## bigdata-analysis-spark
  spark常用案例：
  1.sparkcore的transform、action操作；
  2.sparksql常用案例：大表join小表；小表join大表；
  3.sparkStreaming状态编程wordcount案例；
  4.sparkMlib机器学习案例：分类算法K近邻算法、分类算法朴素贝叶斯算法、决策树与随机森林、线性回归、逻辑回归、聚类算法
