package com.zhang.loganalyzer

import com.zhang.util.AccessLogUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求一：求contentsize的平均值、最小值、最大值
  * 需求二：请各个不同返回值的出现的数据 ===> wordCount程序
  * 需求三：获取访问次数超过N次的IP地址
  * 需求四：获取访问次数最多的前K个endpoint的值 ==> TopN
  *
  */
object LogAnalyzer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LogAnalyzer")
      .setAppName("local[*]")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "hdfs://hadoop01:8020/spark-history")
    val sc = SparkContext.getOrCreate(conf)

    val path = "/spark/access/access.log"

    val rdd = sc.textFile(path)

    val accessLog = rdd.filter(line => AccessLogUtil.isValidateLogLine(line))
      .map(line => {
        //todo:对line数据进行解析
        AccessLogUtil.parseLogLine(line)
      })

    //todo:对多次使用到的rdd进行cache
    accessLog.cache()

    /**
      * 求contentsize的平均值、最小值、最大值
      */
    val contentSizeRDD = accessLog.map(log => (log.contentSize))
    contentSizeRDD.cache()

    //todo:开始计算平均值、最小值、最大值
    val totalContentSize = contentSizeRDD.sum()
    val totalCount = contentSizeRDD.count()
    val avgSize = 1.0 * totalContentSize / totalCount
    val minSize = contentSizeRDD.min()
    val maxSize = contentSizeRDD.max()

    contentSizeRDD.unpersist()

    //todo:结果输出
    println(s"ContentSize Avg：${avgSize}, Min: ${minSize}, Max: ${maxSize}")

    /**
      * 需求二：请各个不同返回值的出现的数据 ===> wordCount程序
      */
    val responseCodeResultRDD = accessLog.map(log => (log.responseCode, 1))
      .reduceByKey(_ + _)

    //todo:结果输出
    println(s"""ResponseCode :${responseCodeResultRDD.collect().mkString(",")}""")

    /**
      * 需求三：获取访问次数超过N次的IP地址
      * 需求三额外：对IP地址进行限制，部分黑名单IP地址不统计
      *
      */
    val blackIP = Array("200-55-104-193.dsl.prima.net.ar",
      "10.0.0.153",
      "208-38-57-205.ip.cal.radiant.net")

    val broadcastIP = sc.broadcast(blackIP)
    val N = 10
    val ipAddressRDD = accessLog.filter(log => !broadcastIP.value.contains(log.ipAddress))
      .map(log => (log.ipAddress, 1L))
      .reduceByKey(_ + _)
      .filter(tuple => tuple._2 > N)
      // 获取满足条件IP地址, 为了展示方便，将下面这行代码注释
      .map(tuple => tuple._1)

    // 结果输出
    println(s"""IP Address :${ipAddressRDD.collect().mkString(",")}""")

    /**
      * 需求四：获取访问次数最多的前K个endpoint的值 ==> TopN
      *
      */
    val K = 10
    val topKValues = accessLog.map(log => (log.endpoint, 1))
      .reduceByKey(_ + _)
      .top(K)(AccessLogUtil.TupleOrdering)
      .map(_._1)

    println(s"""TopK values:${topKValues.mkString(",")}""")

    accessLog.unpersist()

    sc.stop()


  }

}
