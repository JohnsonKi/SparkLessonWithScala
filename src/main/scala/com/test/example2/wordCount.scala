package com.test.example2

import org.apache.spark.{SparkConf,SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // Windows Local File
    val lines = sc.textFile("C:\\MyWorkSpace\\forIDEA\\testData\\\\README.md")

    // Cluster File
    //val liness = sc.textFile("hdfs://master:8020/tmp/test.txt")
    //val liness = sc.textFile("/tmp/test.txt")

    //val words = lines.flatMap(line=>line.split(" "))
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))
    val pairsCountByKey = pairs.reduceByKey(_+_)
    val sortPairs = pairsCountByKey.map(pair=>(pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2,pair._1))

    sortPairs.collect().foreach(pair=>println(pair._1 + " : " + pair._2))
  }
}
