package com.zhang.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

  def FILE_NAME: String = "word_count_results_";

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:SparkWordCount FileName");
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile(args(0));
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(
      word => (word, 1)).reduceByKey((a, b) => a + b)
    //println("Word Count program running results:");
    wordCounts.collect().foreach(e => {
      val (k, v) = e
      println(k + "=" + v)
    });
    wordCounts.saveAsTextFile(FILE_NAME + System.currentTimeMillis());
    println("Word Count program running results are successfully saved.");
  }

}
