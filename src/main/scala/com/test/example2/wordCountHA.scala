package com.test.example2

import org.apache.spark.{SparkConf, SparkContext}

object wordCountHA {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // Windows Local File
//    val lines = sc.textFile("C:\\MyWorkSpace\\forIDEA\\testData\\\\README.md")

    // Cluster File
    val lines = sc.textFile("hdfs://master:8020/tmp/test.txt")
    //val lines = sc.textFile("/tmp/test.txt")

    //val words = lines.flatMap(line=>line.split(" "))
    val words = lines.flatMap(_.split(" ")).filter(_.nonEmpty)
    val pairs = words.map((_,1))
    val pairsCountByKey = pairs.reduceByKey(_+_)
    val sortPairs = pairsCountByKey.map(pair=>(pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2,pair._1))

    // take head 10
    val head10 = sortPairs.take(10)

    head10.foreach(pair=>println(pair._1 + " : " + pair._2))

  }
}
