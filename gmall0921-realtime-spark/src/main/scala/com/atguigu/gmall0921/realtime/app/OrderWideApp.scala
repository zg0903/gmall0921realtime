package com.atguigu.gmall0921.realtime.app

import java.util
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0921.realtime.utils.{HbaseUtil, MykafkaUtil, RedisUtil, offsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.app
 * @author zhuguang
 * @date 2021-01-30-20:32
 */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("order_wide_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val orderInfoTopic = "DWD_ORDER_INFO_I"
    val orderDetailTopic = "DWD_ORDER_DETAIL_I"

    val groupid = "order_wide_group"

    val orderInfoOffsetMap: Map[TopicPartition, Long] = offsetManagerUtil.getOffset(orderInfoTopic, groupid)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = offsetManagerUtil.getOffset(orderDetailTopic, groupid)

    var orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    var orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = null

    if (orderInfoOffsetMap == null) {
      orderInfoInputDstream = MykafkaUtil.getKafkaStream(orderInfoTopic, ssc, groupid)
    } else {
      orderInfoInputDstream = MykafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, groupid)
    }

    if (orderDetailOffsetMap == null) {
      orderDetailInputDstream = MykafkaUtil.getKafkaStream(orderDetailTopic, ssc, groupid)
    } else {
      orderDetailInputDstream = MykafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, groupid)
    }

    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoInputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoInputDstream.transform { rdd =>
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      orderInfoOffsetRanges = hasOffsetRanges.offsetRanges
      rdd
    }
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailInputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailInputDstream.transform { rdd =>
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      orderDetailOffsetRanges = hasOffsetRanges.offsetRanges
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputDstreamWithOffsetDstream.map {
      record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        val create_time: String = orderInfo.create_time
        val creatTimeArr: Array[String] = create_time.split(" ")
        orderInfo.create_date = creatTimeArr(0)
        orderInfo.create_hour = creatTimeArr(1).split(":")(0)
        orderInfo
      }
    }


    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDstreamWithOffsetDstream.map {
      record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        orderDetail
      }
    }


    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoDstream.map { orderInfo =>

      val rowKey: String = HbaseUtil.getDimRowkey(orderInfo.user_id.toString)
      val userInfoJsonObj: JSONObject = HbaseUtil.get("DIM_USER_INFO", rowKey)

      val date: Date = userInfoJsonObj.getDate("birthday")
      val userBirthMills: Long = date.getTime
      val curMills = System.currentTimeMillis()
      orderInfo.user_age = ((curMills - userBirthMills) / 1000 / 60 / 60 / 24 / 365).toInt
      orderInfo.user_gender = userInfoJsonObj.getString("gender")
      orderInfo
    }

    //用driver查询hbase  通过广播变量发放到各个executor
    //    val provinceMap: mutable.Map[String, JSONObject] = HbaseUtil.scanTable("DIM_BASE_PROVINCE")
    //    val provinceBC: Broadcast[mutable.Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)
    //    orderInfoWithUserDstream.map {
    //      orderInfo =>
    //        val provinceMap: mutable.Map[String, JSONObject] = provinceBC.value
    //        val provinceObj: JSONObject = provinceMap.getOrElse(HbaseUtil.getDimRowkey(orderInfo.province_id.toString), null)
    //        orderInfo.province_name = provinceObj.getString("name")
    //
    //    }

    val orderInfoWithDimDstream: DStream[OrderInfo] = orderInfoWithUserDstream.transform {
      rdd =>
        val provinceMap: mutable.Map[String, JSONObject] = HbaseUtil.scanTable("DIM_BASE_PROVINCE")
        val provinceBC: Broadcast[mutable.Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)
        val orderInfoRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
          val provinceMap: mutable.Map[String, JSONObject] = provinceBC.value
          val provinceObj: JSONObject = provinceMap.getOrElse(HbaseUtil.getDimRowkey(orderInfo.province_id.toString), null)
          println("namename")
          orderInfo.province_name = provinceObj.getString("name")
          orderInfo.province_area_code = provinceObj.getString("area_code")
          println("area_code")
          orderInfo.province_iso_code = provinceObj.getString("iso_code")
          orderInfo.province_3166_2_code = provinceObj.getString("iso_3166_2")

          orderInfo

        }
        orderInfoRDD
    }


    val orderInfoWithIdDstream: DStream[(Long, OrderInfo)] = orderInfoWithDimDstream.map(orderInfo => (orderInfo.id, orderInfo))

    val orderDetailWithIdDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))
    val orderJoinDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithIdDstream.join(orderDetailWithIdDstream)


    val orderFullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithIdDstream.fullOuterJoin(orderDetailWithIdDstream)

    val orderWideDStream: DStream[OrderWide] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>
      val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
      val jedis: Jedis = RedisUtil.getJedisClient
      if (orderInfoOption != None) {
        val orderInfo: OrderInfo = orderInfoOption.get
        if (orderDetailOption != None) {
          val orderDetail: OrderDetail = orderDetailOption.get
          val orderWide = new OrderWide(orderInfo, orderDetail)
          orderWideList.append(orderWide)
        }
        val orderInfoKey = "ORDER_INFO:" + orderInfo.id
        val orderInfoJson = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        jedis.setex(orderInfoKey, 600, orderInfoJson)
        val orderDetailKey = "ORDER_DETAIL:" + orderInfo.id
        val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailSet != null && orderDetailSet.size() > 0) {
          import collection.JavaConverters._
          for (orderDetailJson <- orderDetailSet.asScala) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide)
          }
        }
      } else {
        val orderDetail: OrderDetail = orderDetailOption.get
        val orderDetailKey = "ORDER_DETAIL:" + orderDetail.order_id
        val orderDetailJson = JSON.toJSONString(orderDetail, new SerializeConfig(true))
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 600)
        val orderInfoKey = "ORDER_INFO:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.length() > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          val orderWide = new OrderWide(orderInfo, orderDetail)
          orderWideList.append(orderWide)
        }
      }


      jedis.close()
      orderWideList
    }


    //    orderInfoWithDimDstream.print(1000)
    //        orderDetailDstream.print(1000)

    orderWideDStream.print(1000)
    ssc.start()
    ssc.awaitTermination()


  }
}
