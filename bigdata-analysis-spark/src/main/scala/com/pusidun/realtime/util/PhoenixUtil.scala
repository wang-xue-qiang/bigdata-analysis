package com.pusidun.realtime.util

import java.sql._

import scala.collection.mutable.ListBuffer


object PhoenixUtil {

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from  user_status")
    println(list)
  }

  def queryList(sql:String): List[JSONObject] ={
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    //建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while(rs.next()){
      val userStatusJsonObj = new JSONObject()
      //{"user_id":"zs","if_consumerd":"1"}
      for(i <-1 to rsMetaData.getColumnCount){
        userStatusJsonObj.put(rsMetaData.getColumnName(i),rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }
    //释放资源
    rs.close()
    ps.close()
    conn.close()
    rsList.toList
  }
}
