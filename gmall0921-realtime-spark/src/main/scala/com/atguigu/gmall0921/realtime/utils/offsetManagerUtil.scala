package com.atguigu.gmall0921.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.utils
 * @author zhuguang
 * @date 2021-01-23-9:41
 */
object offsetManagerUtil {
  def getOffset(topic: String, grouId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + grouId
    val offsetMapOrigin: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    if (offsetMapOrigin != null && offsetMapOrigin.size() > 0) {

      import collection.JavaConverters._
      val offsetMapForKafka: Map[TopicPartition, Long] = offsetMapOrigin.asScala.map {
        case (partitionStr, offsetStr) =>
          val topicPartition: TopicPartition = new TopicPartition(topic, partitionStr.toInt)
          (topicPartition, offsetStr.toLong)
      }.toMap
      println("读取起始偏移量：：" + offsetMapForKafka)
      offsetMapForKafka

    } else {
      null

    }


  }


  def svaOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + groupId

    val offsetMapForRedis = new util.HashMap[String, String]()
    for (offsetRange <- offsetRanges) {
      val partition: Int = offsetRange.partition //分区
      val offset: Long = offsetRange.untilOffset //偏移量结束点
      offsetMapForRedis.put(partition.toString, offset.toString)
    }
    println("写入偏移量结束点："+offsetMapForRedis)
    jedis.hmset(offsetKey, offsetMapForRedis)
    jedis.close()


  }


}
