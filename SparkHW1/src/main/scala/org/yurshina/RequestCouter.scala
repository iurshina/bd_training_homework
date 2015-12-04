package org.yurshina

import org.apache.spark.{SparkConf, SparkContext}

object RequestCouter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Request counter").setMaster("local")
    val sc = new SparkContext(conf)

    val logData = sc.textFile("/home/nastya/java/Big data training/access_logs/input/000000")

    val Opera: String = "Opera"
    val opAcc = sc.accumulator(0, Opera)
    val Firefox: String = "Mozilla"
    val firefoxAcc = sc.accumulator(0, Firefox)

    val ipParser = new Parser
    val parsedLogs = logData.map(line => ipParser.parseString(line))

    val shuffledLog = parsedLogs.map(log => {
      if(log.browser.contains(Opera)) opAcc += 1
      if(log.browser.contains(Firefox)) firefoxAcc += 1
      (log.ipAddress, log.bytes)
    })

    val result = shuffledLog.mapValues(v => (v, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val rdd = result.map(line => (line._1, line._2._1, line._2._1 / line._2._2))

    rdd.saveAsTextFile("res")

    println(s"Opera: ${opAcc.value}")
    println(s"Mozilla: ${firefoxAcc.value}")
  }
}