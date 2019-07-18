package com.zhang.agecalculator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 该案例中，我们将假设我们需要统计一个 1000 万人口的所有人的平均年龄，
  * 当然如果您想测试 Spark 对于大数据的处理能力，您可以把人口数放的更大，
  * 比如 1 亿人口，当然这个取决于测试所用集群的存储容量。假设这些年龄信息
  * 都存储在一个文件里，并且该文件的格式如下，第一列是 ID，第二列是年龄。
  */
object AvgAgeCalculator {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:AvgAgeCalculator datafile")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("AvgAgeCalculator")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile(args(0), 5);
    val count = dataFile.count()
    val ageData = dataFile.map(line => line.split(" ")(1))
    val totalAge = ageData.map(age => Integer.parseInt(
      String.valueOf(age))).collect().reduce((a, b) => a + b)
    println("Total Age:" + totalAge + ";Number of People:" + count)
    val avgAge: Double = totalAge.toDouble / count.toDouble
    println("Average Age is " + avgAge)
  }

}
