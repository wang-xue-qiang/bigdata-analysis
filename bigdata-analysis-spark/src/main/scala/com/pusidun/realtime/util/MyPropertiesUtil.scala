package com.pusidun.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 读取配置文件工具类
 */
object MyPropertiesUtil {

  def main(args: Array[String]): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
  }

  /**
   * 核心方法
   *
   * @param propertiesName 配置文件名
   * @return
   */
  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    //加载指定的配置文件
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8))
    prop
  }
}
