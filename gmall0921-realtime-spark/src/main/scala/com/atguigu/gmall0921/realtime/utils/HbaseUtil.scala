package com.atguigu.gmall0921.realtime.utils

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall0921.realtime.utils.HbaseUtil.convertToJSONObj
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable


/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.utils
 * @author zhuguang
 * @date 2021-01-30-9:32
 */
object HbaseUtil {
  var connection: Connection = null
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val HBASE_SERVER: String = properties.getProperty("hbase.server")
  private val DEFAULT_FAMILY: String = properties.getProperty("hbase.default.family")
  private val NAMESPACE: String = properties.getProperty("hbase.namespace")


  def get(tableName: String, rowKey: String): JSONObject = {
    if (connection == null) init() //连接
    //连接 -->表
    val table: Table = connection.getTable(TableName.valueOf(NAMESPACE + ":" + tableName))
    //创建查询动作
    val get = new Get(Bytes.toBytes(rowKey))
    //执行查询
    val result: Result = table.get(get)
    //转换查询结果
    convertToJSONObj(result)
  }

  def convertToJSONObj(result: Result): JSONObject = {
    val cells: Array[Cell] = result.rawCells()
    val jsonObj = new JSONObject()
    for (cell <- cells) {
      jsonObj.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)))
    }
    jsonObj
  }


  def scanTable(tableName: String): mutable.Map[String, JSONObject] = {
    if (connection == null) init() //连接
    //连接 -->表
    val table: Table = connection.getTable(TableName.valueOf(NAMESPACE + ":" + tableName))
    val scan = new Scan()
    val resultScanner: ResultScanner = table.getScanner(scan)
    // 把整表结果存入Map中
    val resultMap: mutable.Map[String, JSONObject] = mutable.Map[String, JSONObject]()
    import collection.JavaConverters._
    for (result <- resultScanner.iterator().asScala) {
      val rowkey: String = Bytes.toString(result.getRow)
      val jsonObj: JSONObject = convertToJSONObj(result)
      resultMap.put(rowkey, jsonObj)
    }
    resultMap
  }

  def getDimRowkey(id: String): String = {
    StringUtils.leftPad(id, 10, "0").reverse
  }


  def init() = {
    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", HBASE_SERVER)

    connection = ConnectionFactory.createConnection(configuration)

    println("hbase 初始化成功")
  }

  def put(tableName: String, rowKey: String, columnValueMap: java.util.Map[String, AnyRef]) = {
    if (connection == null) init();
    val table: Table = connection.getTable(TableName.valueOf(NAMESPACE + ":" + tableName))
    val puts: util.List[Put] = new java.util.ArrayList[Put]()
    import collection.JavaConverters._
    for (colValue <- columnValueMap.asScala) {
      val col: String = colValue._1
      val value: AnyRef = colValue._2
      if (value != null && value.toString.length > 0) {

        puts.add(new Put(Bytes.toBytes(rowKey)).addColumn(Bytes.toBytes(DEFAULT_FAMILY), Bytes.toBytes(col
        ), Bytes.toBytes(value.toString)))
      }
    }
    table.put(puts)
  }

}
