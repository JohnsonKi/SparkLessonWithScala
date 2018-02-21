package com.test.example2

import org.apache.spark.{Accumulable, Accumulator, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object questionnnaire {
  def main(args:Array[String]) = {
    val sc = getSparkContext("Questionnaire", "local")
    val questionnaireRDD = sc.textFile("C:\\MyWorkSpace\\forIDEA\\testData\\questionnaire.csv")
    val pairsRDD = questionnaireRDD.map { line =>
      val lineArr = line.split(",")
      val ageRange = lineArr(0).toInt
      val maleOrFemale = lineArr(1)
      val point = lineArr(2).toInt
      (ageRange, maleOrFemale, point)
    }

    val cachedRDD = pairsRDD.cache()
    //    getAvgType1(cachedRDD)
    //    getAvgType2(cachedRDD)
    //    getAgeAvgType1(cachedRDD)
    //    getSexAvgType1(cachedRDD)
    getSexAccumulator(sc, cachedRDD)
    sc.stop()
  }

  def getSexAccumulator(sc: SparkContext, cachedRDD:RDD[(Int, String, Int)]) {
    var numMAcc:Accumulator[Int] = sc.accumulator(0, "Number Of M")
    var totalPointMAcc:Accumulator[Int] = sc.accumulator(0, "Total Point Of M")
    var numFAcc:Accumulator[Int] = sc.accumulator(0, "Number Of F")
    var totalPointFAcc:Accumulator[Int] = sc.accumulator(0, "Total Point Of M")

    val retRDD = cachedRDD.foreach{
      case (_, maleOfFemale, point) =>
        maleOfFemale match {
          case "M" => {
            numMAcc += 1
            totalPointMAcc += point
          }
          case "F" => {
            numFAcc += 1
            totalPointFAcc += point
          }
        }
    }

    println(s"AVG Male: ${totalPointMAcc.value / numMAcc.value.toDouble}")
    println(s"AVG Female: ${totalPointFAcc.value / numFAcc.value.toDouble}")
  }

  def getSexAvgType1(cachedRDD:RDD[(Int, String, Int)]) {
    val sexPointRDD = cachedRDD.map(line => (line._2, (line._3, 1)))
    sexPointRDD.collect.foreach(println)
    println("=====================")

    val totalPtAndCntPerSexRDD = sexPointRDD.reduceByKey{
      case ((sexPt, sexCnt), (point, cnt)) =>
        (sexPt+point, sexCnt+cnt)
    }

    println("=====================")
    totalPtAndCntPerSexRDD.collect.foreach{
      case (sexRange, (totalPoint, count)) =>
        println(s"AVG sex Range($sexRange): ${totalPoint/count.toDouble}")
    }

  }

  def getAgeAvgType1(cachedRDD:RDD[(Int, String, Int)]) {
    val agePointRDD = cachedRDD.map(line => (line._1, (line._3, 1)))
    agePointRDD.collect.foreach(println)
    println("=====================")

    val totalPtAndCntPerAgeRDD = agePointRDD.reduceByKey{
      case ((intermediaPoint, intermediaCount), (point, one)) =>
        println(s"intermediaPoint=$intermediaPoint, intermediaCount=$intermediaCount, point=$point, one=$one")
        (intermediaPoint + point, intermediaCount + one)
    }

    println("=====================")
    totalPtAndCntPerAgeRDD.collect.foreach{
      case (ageRange, (totalPoint, count)) =>
        println(s"AVG age Range($ageRange): ${totalPoint/count.toDouble}")
    }

  }

  def getAvgType2(cachedRDD:RDD[(Int, String, Int)]) {

    val test1 = cachedRDD.map(line => (line._3, 1))
    test1.collect.foreach(println)
    println("=====================")

    val (totalPoints, numQuestionnaire) = cachedRDD.map(line => (line._3, 1)).reduce{
      case ((intermediaPoint, intermediaCount), (point, one)) =>
        println(s"intermediaPoint=$intermediaPoint, intermediaCount=$intermediaCount, point=$point, one=$one")
        (intermediaPoint + point, intermediaCount + one)
    }

    println("=====================")
    println(s"Avg all: ${totalPoints/numQuestionnaire}")

  }

  def getAvgType1(cachedRDD:RDD[(Int, String, Int)]) {
    val numQuestionnaire = cachedRDD.count()
    val totalPoints = cachedRDD.map(x=>x._3).sum()
    println(s"Avg all: ${totalPoints/numQuestionnaire}")

  }

  def getSparkContext(name:String, url:String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(url)
    conf.setAppName(name)

    val sc = new SparkContext(conf)
    sc
  }
}
