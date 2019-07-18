package com.zhang.util

import java.io.{File, FileWriter}

import scala.util.Random

object SampleDataFileGenerator {
  def main(args: Array[String]): Unit = {
    val writer = new FileWriter(new File("D:\\原始数据\\sample_age_data.txt"), false)
    val rand = new Random()
    for (i <- 1 to 10000000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}

