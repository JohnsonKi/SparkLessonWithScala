package com.test.example2

import scala.collection.mutable.{HashMap}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object BestSellerFinder {
  def main(args: Array[String]) = {

    val salesCSVFile1 = "C:\\MyWorkSpace\\forIDEA\\testData\\sales-october.csv"
    val salesCSVFile2 = "C:\\MyWorkSpace\\forIDEA\\testData\\sales-november.csv"
    val productCSVFile = "C:\\MyWorkSpace\\forIDEA\\testData\\products.csv"

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("BestSellerFinder1")

    val sc = new SparkContext(conf)

    val salesRDD1 = sc.textFile(salesCSVFile1)
    val pairs1 = salesRDD1.map { line =>
      val lineColumns = line.split(",")
      val productId = lineColumns(2)
      val soldCount = lineColumns(3).toInt
      (productId, soldCount)
    }
    val sold1 = pairs1.reduceByKey(_+_)
    // sold1.foreach(pair => println("sales1 -> [" + pair._1 + ":" + pair._2 + "]"))

    val salesRDD2 = sc.textFile(salesCSVFile2)
    val pairs2 = salesRDD2.map { line =>
      val lineColumns = line.split(",")
      val productId = lineColumns(2)
      val soldCount = lineColumns(3).toInt
      (productId, soldCount)
    }
    val sold2 = pairs2.reduceByKey(_+_)
    //sold2.foreach(pair => println("sales2 -> [" + pair._1 + ":" + pair._2 + "]"))

    val allSold = sold1.join(sold2).map{
      case (productId, (count1, count2)) => (productId, count1+count2)
    }
    // allSold.foreach(pair => println("allSold -> [" + pair._1 + ":" + pair._2 + "]"))

    val products = Source.fromFile(productCSVFile)

    val productsMap = new HashMap[String, (String, Int)]
    for (line <- products.getLines()) {
      val productColumns = line.split(",")
      val productId = productColumns(0)
      val productName = productColumns(1)
      val unitPrice = productColumns(2).toInt
      productsMap(productId) = (productName, unitPrice)
    }

    //
    val bdMap = sc.broadcast(productsMap)

    val resultRDD = allSold.map{
      case (productId, amount) => {
        val productsKV = bdMap.value
        val (productName, unitPrice) = productsKV(productId)
        (productName, amount, amount * unitPrice)
      }
    }
    resultRDD.foreach(pair => println("allSold -> [" + pair._1 + ":" + pair._2 + ":" + pair._3 + "]"))

//    resultRDD.saveAsTextFile("C:\\MyWorkSpace\\forIDEA\\testData\\bestseller-output")

    sc.stop()
  }
}
