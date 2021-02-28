package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//TODO  sparkstreaming消费kafka数据

object DauApp {
  def main(args: Array[String]): Unit = {

    //1、sparkstreaming环境配置
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2、通过工具类，获取kafka数据流
    val topic = "ODS_BASE_LOG"
    val groupId = "dau_group"
    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

//    inputStream.map(_.value()).print(100)   测试输出

    //3、统计用户的当日访问 DAU uv
    //    1、可以通过判断： 日志中page栏是否有last_page_id来决定该页面为 ---> 一次访问会话的首个页面
    //    2、也可以通过启动日志来判断： 是否是首次访问

    //TODO  先转换格式，转换成方便操作的jsonObj
    val logJsonDstream: DStream[JSONObject] = inputStream.map { //---大括号
      record =>
        val jsonString: String = record.value()
        val logJsonObj: JSONObject = JSON.parseObject(jsonString)

        //把ts转换成日期  和  小时
        val ts: Long = logJsonObj.getLong("ts")
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHourString: String = format.format(new Date(ts))
        val strings: Array[String] = dateHourString.split(" ")
        val dt = strings(0)
        val hr = strings(1)
        logJsonObj.put("dt", dt)
        logJsonObj.put("hr", hr)
        logJsonObj
    }

    //TODO  过滤
    val firstPageJsonDstream: DStream[JSONObject] = logJsonDstream.filter {
      logJsonObj =>
        var isFirstPage = false
        val pageJsonObject: JSONObject = logJsonObj.getJSONObject("page")
        if (pageJsonObject != null) {
          val lastPageId: String = pageJsonObject.getString("last_page_id")
          if (lastPageId == null) {
            isFirstPage = true
          }
        }
        isFirstPage
    }
    firstPageJsonDstream.print(100)


    //要把访问会话次数  ---TODO   去重---  当日首次访问（会话）
    //	将状态存在Redis中   	存在关系型数据库中   	通过Spark自身的updateStateByKey
    val dauJsonDstream: DStream[JSONObject] = firstPageJsonDstream.filter {
      logJsonObj =>
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        val dauKey = "dau" + logJsonObj.getString("dt")
        val mid: String = logJsonObj.getJSONObject("common").getString("mid")
        val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid)
        if (isFirstVisit == 1L) {
          println("用户" + mid + "首次访问，保留！")
          true
        } else {
          println("用户" + mid + "重复访问，去掉！")
          false
        }
    }
    dauJsonDstream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
