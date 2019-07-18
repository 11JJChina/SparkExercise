package com.zhang.projectcount

import java.net.URL

import com.zhang.util.ProjetPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如下图的文件中有很多访问记录，第一列表示访问站点的时间戳，第二列表示访问的站点，
  * 中间用制表符分割。这里相当于学习的不同课程，如java，ui，bigdata，android，h5
  * 等，其中每门课程又分为子课程，如h5课程分为teacher，course等。现在需要统计每门
  * 课程，学习量最高的两门子课程并降序排列
  * 数据格式如下:
  * 20161123101523  http://java.learn.com/java/javaee.shtml
  * 20161123101523  http://bigdata.learm.com/bigdata.shtml
  *
  */
object ProjectCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProjectCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val file = sc.textFile("D:\\原始数据\\access.txt")

    val urlAndOne: RDD[(String, Int)] = file.map(line => {
      val fields = line.split("\t")
      val url = fields(1)
      (url, 1)
    })

    val sumUrl = urlAndOne.reduceByKey(_ + _)

    val cacheProject: RDD[(String, (String, Int))] = sumUrl.map(line => {
      val url = line._1
      val count = line._2
      val project = new URL(url).getHost
      (project, (url, count))
    }).cache()

    val project: Array[String] = cacheProject.keys.distinct().collect()

    //todo:调用自定义分区器并得到分区号
    val partitioner = new ProjetPartitioner(project)

    //todo:分区
    val partitioned = cacheProject.partitionBy(partitioner)

    //todo:每个分区的数据进行排序并取top2
    val ans: RDD[(String, (String, Int))] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    ans.saveAsTextFile("D:\\原始数据\\out")
    sc.stop()


  }
}

