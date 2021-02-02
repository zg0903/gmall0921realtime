package com.atguigu.gmall0921.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall0921.realtime.utils.{HbaseUtil, MyKafkaSink, MykafkaUtil, offsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.app
 * @author zhuguang
 * @date 2021-01-29-11:51
 */
object BaseDBCanalApp1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_ad_canal_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_DB_C"
    val groupid = "base_db_canal_group"

    val offsetMap: Map[TopicPartition, Long] = offsetManagerUtil.getOffset(topic, groupid)
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap == null) {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, groupid)
    } else {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupid)
    }

    var offsetRanges: Array[OffsetRange] = null
    val inputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      offsetRanges = hasOffsetRanges.offsetRanges
      rdd
    }

    val jsonObjDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map(record => JSON.parseObject(record.value()))

    jsonObjDstream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          jsonItr =>
            for (jsonObj <- jsonItr) {
              val dimTables = Array("user_info", "base_province")
              val factTable = Array("order_info", "order_detail")

              val table: String = jsonObj.getString("table")
              val optType: String = jsonObj.getString("type")
              val pkNames: JSONArray = jsonObj.getJSONArray("pkNames")
              val dataArr: JSONArray = jsonObj.getJSONArray("data")

              if (dimTables.contains(table)) {
                // 维度处理 hbase
                val pkName: String = pkNames.getString(0)
                import collection.JavaConverters._
                for (data <- dataArr.asScala) {
                  val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
                  println(dataJsonObj)
                  val pk: String = dataJsonObj.getString(pkName)
                  val rowkey: String = StringUtils.leftPad(pk, 10, "0").reverse

                  val hbaseTable: String = "DIM_" + table.toUpperCase
                  val dataMap: util.Map[String, AnyRef] = dataJsonObj.getInnerMap
                  HbaseUtil.put(hbaseTable, rowkey, dataMap)


                }

              }
              if (factTable.contains(table)) {
                //分流到事实表处理 kafka
                //事实表   topic 层+table+optype   message  ? data
                var opt: String = null
                if (optType.equals("INSERT")) {
                  opt = "I"
                } else if (optType.equals("UPDATE")) {
                  opt = "U"
                } else if (optType.equals("DELETE")) {
                  opt = "D"
                }
                val topic = "DWD_" + table.toUpperCase() + "_" + opt
                import collection.JavaConverters._
                for (data <- dataArr.asScala) {
                  val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
                  println(dataJsonObj)
                  MyKafkaSink.send(topic, dataJsonObj.toJSONString)
                }
              }
            }
        }
        offsetManagerUtil.svaOffset(topic, groupid, offsetRanges)


      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
