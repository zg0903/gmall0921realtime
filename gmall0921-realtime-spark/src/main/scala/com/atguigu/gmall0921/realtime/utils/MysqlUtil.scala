package com.atguigu.gmall0921.realtime.utils

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util

import com.alibaba.fastjson.{JSONObject, JSONPObject}

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.utils
 * @author zhuguang
 * @date 2021-01-30-16:41
 */
object MysqlUtil {

  def main(args: Array[String]): Unit = {
    val objects: util.List[JSONObject] = queryList("select * from base_province")
    val objects1: util.List[JSONObject] = queryList("select * from user_info")
    //    println(objects)
    println(objects)
  }


  def queryList(sql: String): java.util.List[JSONObject] = {
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: java.util.List[JSONObject] = new java.util.ArrayList[JSONObject]()
    val conn: Connection = DriverManager
      .getConnection("jdbc:mysql://hadoop102:3306/gmall0921?characterEncoding=utf-8&useSSL=false", "root", "mima0903!")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next) {
      val rowData = new JSONObject();
      for (i <- 1 to md.getColumnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList.add(rowData)
    }

    stat.close()
    conn.close()
    resultList
  }

}

