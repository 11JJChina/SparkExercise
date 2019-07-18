package com.zhang.util


import org.apache.spark.Partitioner

import scala.collection.mutable

class ProjetPartitioner(projects: Array[String]) extends Partitioner {
  //用来存放学科和分区号
  private val projectsAndPartNum = new mutable.HashMap[String, Int]
  //计数器，用于指定分区号
  var n = 0

  for (pro <- projects) {
    //HashMap插入
    projectsAndPartNum += (pro -> n)
    n += 1
  }

  //得到分区数
  override def numPartitions: Int = projects.length

  //得到分区号
  override def getPartition(key: Any): Int = {
    projectsAndPartNum.getOrElse(key.toString, 0)
  }
}
