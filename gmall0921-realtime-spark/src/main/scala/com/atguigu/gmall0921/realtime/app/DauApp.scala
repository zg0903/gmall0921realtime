package com.atguigu.gmall0921.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.bean.DauInfo
import com.atguigu.gmall0921.realtime.utils.{MyEsUtil, MykafkaUtil, RedisUtil, offsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.app
 * @author zhuguang
 * @date 2021-01-20-20:20
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_LOG"
    val groupid = "dau_app_group"

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


    //    val jsonStringDstream: DStream[String] = inputDstream.map(_.value)
    //    jsonStringDstream.print()
    val jsonObjDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map {
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
      if (pageJsonObj != null) {
        val lastPageId: String = pageJsonObj.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          true
        } else {
          false
        }
      } else {
        false
      }

    }

    firstPageJsonObjDstream.cache()
    firstPageJsonObjDstream.count().print()
    /*
        //去重 以什么字段为准进行去重  用redis来储存已访问列表 什么数据对象来储存列表
        val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.filter { jsonObj =>
          //提取mid
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          val dt: String = jsonObj.getString("dt")
          //查询列表中是否有mid

          //设计定义 已访问列表

          //redis type  string  （每个mid每天一个key）

          //            val jedis = new Jedis("hadoop102", 6379)
          val jedis = RedisUtil.getJedisClient
          val key = "dau:" + dt
          val isNew: lang.Long = jedis.sadd(key, mid)
          jedis.expire(key, 3600 * 24)
          jedis.close()

          //古国有 过滤该对象 没有 保留 插入到列表中
          if (isNew == 1L) {
            true
          } else {
            false
          }
        }
    //    dauDstream.print(1000)


    */
    val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.mapPartitions { jsonObjItr =>
      val jedis = RedisUtil.getJedisClient //该批次 该分区 执行一次
      val filteredList: ListBuffer[JSONObject] = ListBuffer[JSONObject]()
      for (jsonObj <- jsonObjItr) { //条为单位处理
        //提取对象中的mid
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dt: String = jsonObj.getString("dt")
        // 查询 列表中是否有该mid
        // 设计定义 已访问设备列表

        //redis   type? string （每个mid 每天 成为一个key,极端情况下的好处：利于分布式  ）   set √ ( 把当天已访问存入 一个set key  )   list(不能去重,排除) zset(不需要排序，排除) hash（不需要两个字段，排序）
        // key? dau:[2021-01-22] (field score?)  value?  mid   expire?  24小时  读api? sadd 自带判存 写api? sadd
        val key = "dau:" + dt
        val isNew: lang.Long = jedis.sadd(key, mid)
        jedis.expire(key, 3600 * 24)

        // 如果有(非新)放弃    如果没有 (新的)保留 //插入到该列表中
        if (isNew == 1L) {
          filteredList.append(jsonObj)
        }
      }
      jedis.close()
      filteredList.toIterator
    }



    //    dauDstream.count().print()


    dauDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonObjItr =>
        //        for (jsonObj <- jsonObjItr) {
        //          println(jsonObj)
        //        }
        val docList: List[JSONObject] = jsonObjItr.toList
        if (docList.size > 0) {
          val dateFormat = new SimpleDateFormat("yyyyMMdd")
          val dt: String = dateFormat.format(new Date())
          val docWithIdList: List[(String, DauInfo)] = docList.map {
            jsonObj => {
              val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
              val mid: String = commonJsonObj.getString("mid")
              val uid: String = commonJsonObj.getString("uid")
              val ar: String = commonJsonObj.getString("ar")
              val ch: String = commonJsonObj.getString("ch")
              val vc: String = commonJsonObj.getString("vc")
              val dt: String = jsonObj.getString("dt")
              val hr: String = jsonObj.getString("hr")
              val ts: Long = jsonObj.getLong("ts")
              val dauInfo = DauInfo(mid, uid, ar, ch, vc, dt, hr, ts)
              (mid, dauInfo)
            }
          }
          //批量保存
          MyEsUtil.saveBulk("gmall0921_dau_info_" + dt, docWithIdList)
        }
      }
      offsetManagerUtil.svaOffset(topic, groupid, offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()


  }

}
