package org.yurshina

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable

object Main extends Logging {

  def main(args: Array[String]) {
    setStreamingLogLevels()

    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming example")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoints")

    implicit val sqlContext = new HiveContext(ssc.sparkContext)

    val limitSettings = HiveHelper.load(2)
    val thresholdSettings = HiveHelper.load(1)
    val dstream = ssc.receiverStream(new PCap4jReceiver())

    //check the specified ips
    var specifiedLimitIps = mutable.MutableList[String]()
    limitSettings.foreach {
      e => {
        val ip = e._1
        val period = e._2.period
        val limit = e._2.value

        TrafficAnalyzer.checkForLimit(dstream, (packet: NetworkPacket) => packet.ip.equals(ip), Minutes(period), limit)
        specifiedLimitIps += ip
      }
    }

    var specifiedThrshIps = mutable.MutableList[String]()
    limitSettings.foreach {
      e => {
        val ip = e._1
        val period = e._2.period
        val limit = e._2.value

        TrafficAnalyzer.checkForThreshold(dstream, (packet: NetworkPacket) => packet.ip.equals(ip), Minutes(period),
          limit)
        specifiedThrshIps += ip
      }
    }

    //check the rest with default values
    val defaultLimSettings = limitSettings.get("NULL").get
    val defaultThSettings = thresholdSettings.get("NULL").get

    TrafficAnalyzer.checkForLimit(dstream, (packet: NetworkPacket) => !specifiedLimitIps.contains(packet.ip),
      Minutes(defaultLimSettings.period), defaultLimSettings.value)
    TrafficAnalyzer.checkForThreshold(dstream, (packet: NetworkPacket) => !specifiedThrshIps.contains(packet.ip),
      Minutes(defaultThSettings.period), defaultThSettings.value)
    TrafficAnalyzer.hourlyStatistics(dstream)

    ssc.start()
    ssc.awaitTermination()
  }

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the logging level
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
