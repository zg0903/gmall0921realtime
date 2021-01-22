package com.atguigu.gmall0921.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.utils.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.app
 * @author zhuguang
 * @date 2021-01-20-20:20
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_LOG"
    val groupid = "dau_app_group"
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(topic, ssc, groupid)
    //    val jsonStringDstream: DStream[String] = inputDstream.map(_.value)
    //    jsonStringDstream.print()
    val jsonObjDstream: DStream[JSONObject] = inputDstream.map {
      record => {
        val jsonString: String = record.value()
        val jSONObject: JSONObject = JSON.parseObject(jsonString)
        val ts: lang.Long = jSONObject.getLong("ts")
        val dateHoursStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dataHour: Array[String] = dateHoursStr.split(" ")
        val date: String = dataHour(0)
        val hour: String = dataHour(1)
        jSONObject.put("dt", date)
        jSONObject.put("hr", hour)
        jSONObject

      }
    }

    val firstPageJsonObjDstream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
      val pageJsonObj: JSONObject = jsonObj.getJSONObject("page")
      val lastPageId: String = pageJsonObj.getString("last_page_id")
      if (lastPageId == null || lastPageId.length == 0) {
        true
      } else {
        false
      }

    }

    //去重 以什么字段为准进行去重  用redis来储存已访问列表 什么数据对象来储存列表
    firstPageJsonObjDstream.filter { jsonObj =>
      val mid: String = jsonObj.getJSONObject("common").getString("mid")


      false
    }


    ssc.start()
    ssc.awaitTermination()


  }

}
