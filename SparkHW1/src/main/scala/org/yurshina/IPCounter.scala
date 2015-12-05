package org.yurshina

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object IPCounter {

  def count(sc: SparkContext, logData: RDD[String]): RDD[(String, Long, Long)] = {
    val Opera: String = "Opera"
    val opAcc = sc.accumulator(0, Opera)
    val Firefox: String = "Mozilla"
    val firefoxAcc = sc.accumulator(0, Firefox)

    val ipParser = new Parser
    val parsedLogs = logData.map(line => ipParser.parseString(line))

    val shuffledLog = parsedLogs.map(log => {
      if (log.browser.contains(Opera)) opAcc += 1
      if (log.browser.contains(Firefox)) firefoxAcc += 1
      (log.ipAddress, log.bytes)
    })

    val result = shuffledLog.mapValues(v => (v, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println(s"Opera: ${opAcc.value}")
    println(s"Mozilla: ${firefoxAcc.value}")

    result.map(line => (line._1, line._2._1, line._2._1 / line._2._2))
  }

  case class Log(ipAddress: String, bytes: Long, browser: String)

  class Parser extends java.io.Serializable {

    def parseString(line: String): Log = {
      val tokens = line.split("\\s+")
      try {
        new Log(tokens(0), if (tokens.length < 10) 0L else tokens(9).toLong, if (tokens.length > 11) tokens(11) else "")
      } catch {
        case e: NumberFormatException => new Log("", 0L, "")
      }
    }
  }
}
