package com.test.example3

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object Streaming1 {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setAppName("Streaming - test1")
    // conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" ")).filter(_.nonEmpty)
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
