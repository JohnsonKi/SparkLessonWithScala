package com.test.example2

import org.apache.spark.{SparkConf, SparkContext}

object transformationMethods {
  def main(args: Array[String]) = {

    val sc = SparkContext("Transformations", "local")
    //    mapTransformation(sc)
    //    flatMapTransformation(sc)
    //    groupByKeyTransformation(sc)
    //    reduceByKeyTransformation(sc)
    //    joinTransformation(sc)
    cogroupTransformation(sc)
    sc.stop()

  }

  def SparkContext(name:String, url:String) = {
    val conf = new SparkConf().setAppName(name).setMaster(url)
    val sc = new SparkContext(conf)
    sc
  }

  def mapTransformation(sc:SparkContext) = {
    val numbers = sc.parallelize(1 to 10)
    val mapped = numbers.map(item => 2 * item)
    val filtered = mapped.filter(item => item %2 == 0)
    filtered.collect.foreach(println)
  }

  def flatMapTransformation(sc:SparkContext) = {
    val bigData = Array("Spark", "Scala", "Java Runtime", "Tachyon", "Hadoop HDFS")
    val bigDataStr = sc.parallelize(bigData)
    val bigDataRet = bigDataStr.flatMap(line => line.split(" "))
    bigDataRet.collect.foreach(println)
  }

  def groupByKeyTransformation(sc:SparkContext) = {
    val data = Array(Tuple2(100, "Spark"), Tuple2(90, "Tachyon"), Tuple2(98, "Scala"), Tuple2(100, "Hadoop"))
    val dataRdd = sc.parallelize(data)
    val grouped = dataRdd.groupByKey() // (100,CompactBuffer(Spark, Hadoop)) group value by key, value is sets
    grouped.collect.foreach(println)
  }

  def reduceByKeyTransformation(sc:SparkContext) = {
    val data = Array(Tuple2(100, 1), Tuple2(90, 1), Tuple2(98, 1), Tuple2(100, 1))
    val dataRdd = sc.parallelize(data)
    val reduced = dataRdd.reduceByKey(_+_)
    reduced.collect.foreach(println)
  }

  def joinTransformation(sc:SparkContext) = {
    val studentNames = Array(Tuple2(1, "Spark"), Tuple2(2, "Hadoop"), Tuple2(3, "Scala"), Tuple2(4, "Flink"))

    val studentScores = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 93),
      Tuple2(4, 80)
    )

    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentScores)

    val nameAndScores = names.join(scores).sortByKey(true)
    nameAndScores.collect.foreach(println)
  }

  def cogroupTransformation(sc:SparkContext) = {
    val studentNames = Array(
      Tuple2(1, "Spark"),
      Tuple2(2, "Hadoop"),
      Tuple2(3, "Scala"),
      Tuple2(2, "Hadoop"),
      Tuple2(3, "Scala"),
      Tuple2(4, "Flink"))

    val studentScores = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 93),
      Tuple2(1, 80),
      Tuple2(3, 63),
      Tuple2(2, 60),
      Tuple2(3, 93),
      Tuple2(4, 80)
    )

    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentScores)

    val nameAndScores = names.cogroup(scores).sortByKey() // (2,(CompactBuffer(Hadoop, Hadoop),CompactBuffer(90, 60)))
    nameAndScores.collect.foreach(println)
    nameAndScores.collect.foreach(line => {
      println("ID:" + line._1)
      println("Name:" + line._2._1.mkString(""))
      println("Score:" + line._2._2.mkString(""))
      println("===============")
    })
  }
}
