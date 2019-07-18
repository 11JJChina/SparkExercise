package com.zhang.peopleinfo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 假设我们需要对某个省的人口 (1 亿) 性别还有身高进行统计，需要计
  * 算出男女人数，男性中的最高和最低身高，  * 以及女性中的最高和最
  * 低身高。本案例中用到的源文件有以下格式, 三列分别是 ID，性别，身高 (cm)
  */

object PeopleInfoCalculator {
  def main(args: Array[String]): Unit = {
//    if (args.length < 1) {
//      println("Usage:PeopleInfoCalculator datafile")
//      System.exit(1)
//    }
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("PeopleInfoCalculator").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile("D:\\原始数据\\sample_people_info.txt", 5)

    val maleData = dataFile.filter(line => line.contains("M"))
      .map(line => line.split(" ")(1) + " " + line.split(" ")(2))

    val femaleData = dataFile.filter(line => line.contains("F"))
      .map(line => line.split(" ")(1) + " " + line.split(" ")(2))

    maleData.collect().take(10).foreach { x => println(x) }
    femaleData.collect().take(10).foreach { x => println(x) }

    val maleHeightData = maleData.map(line => line.split(" ")(1).toInt)
    val femaleHeightData = femaleData.map(line => line.split(" ")(1).toInt)

    maleHeightData.collect().take(10).foreach { x => println(x) }
    femaleHeightData.collect().take(10).foreach { x => println(x) }

    val lowestMale = maleHeightData.sortBy(x => x,true).first()
    val lowestFemale = femaleHeightData.sortBy(x => x, true).first()

    maleHeightData.collect().take(10).sortBy(x => x).foreach { x => println(x) }
    femaleHeightData.collect().take(10).sortBy(x => x).foreach { x => println(x) }
    val highestMale = maleHeightData.sortBy(x => x, false).first()
    val highestFemale = femaleHeightData.sortBy(x => x, false).first()
    println("Number of Male Peole:" + maleData.count())
    println("Number of Female Peole:" + femaleData.count())
    println("Lowest Male:" + lowestMale)
    println("Lowest Female:" + lowestFemale)
    println("Highest Male:" + highestMale)
    println("Highest Female:" + highestFemale)


  }

}
