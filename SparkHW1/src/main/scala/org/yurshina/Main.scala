package org.yurshina

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Request counter").setMaster("local")
    val sc = new SparkContext(conf)

    val logData = sc.textFile("/home/nastya/java/Big data training/access_logs/input/000000")

    val res = IPCounter.count(sc, logData)

    res.saveAsTextFile("res")
  }
}