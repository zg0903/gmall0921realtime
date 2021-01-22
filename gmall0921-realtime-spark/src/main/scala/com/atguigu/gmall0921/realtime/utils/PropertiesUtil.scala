package com.atguigu.gmall0921.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.utils
 * @author zhuguang
 * @date 2021-01-20-20:26
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties =  PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
      getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }


}
