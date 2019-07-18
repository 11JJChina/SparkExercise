package com.zhang.topk

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 假设某搜索引擎公司要统计过去一年搜索频率最高的 K 个科技关键词或词组，
  * 为了简化问题，我们假设关键词组已经被整理到一个或者多个文本文件中，并
  * 且文档具有以下格式
  */
object TopKSearchKeyWords {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage:TopKSearchKeyWords KeyWordsFile K")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("TopKSearchKeyWords")
    val sc = new SparkContext(conf)
    val srcData = sc.textFile(args(0))

    val countData = srcData.map(line => (line.toLowerCase, 1)).reduceByKey((a, b) => a + b)

    countData.take(10).foreach(x => println(x))

    val soredData = countData.map {
      case (k, v) =>
        (v, k)
    }.sortByKey(false)

    val topKData = soredData.take(args(1).toInt).map {
      case (v, k) =>
        (k, v)
    }

    topKData.foreach(println)


  }

}
