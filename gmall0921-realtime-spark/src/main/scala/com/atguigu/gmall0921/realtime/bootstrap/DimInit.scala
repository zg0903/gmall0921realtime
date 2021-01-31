package com.atguigu.gmall0921.realtime.bootstrap

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.bootstrap
 * @author zhuguang
 * @date 2021-01-31-13:48
 */


import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.atguigu.gmall0921.realtime.utils.{MyKafkaSink, MysqlUtil, PropertiesUtil}


object DimInit {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("diminit.properties")
    val tableNames: String = properties.getProperty("bootstrap.tablenames")
    val topic = properties.getProperty("bootstrap.topic")
    val tableNameArr: Array[String] = tableNames.split(",")
    for (tableName <- tableNameArr) {
      val dataObjList: util.List[JSONObject] = MysqlUtil.queryList("select * from " + tableName) //无法获知主键
      val messageJSONobj = new JSONObject()
      messageJSONobj.put("data", dataObjList)

      messageJSONobj.put("database", "gmall0921")

      val pkNames = new JSONArray
      pkNames.add("id")

      messageJSONobj.put("pkNames", pkNames)
      messageJSONobj.put("table", tableName)
      messageJSONobj.put("type", "INSERT")
      println(messageJSONobj)
      MyKafkaSink.send(topic, messageJSONobj.toString)
    }

    MyKafkaSink.close()
  }

}

