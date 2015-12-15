package org.yurshina

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import org.yurshina.HiveHelper.Settings

import scala.collection.mutable

object SnifferDriver extends Logging {

  def main(args: Array[String]) {
    setStreamingLogLevels()

    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming example")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoints")

    implicit val sqlContext = new HiveContext(ssc.sparkContext)

    val limitSettings = HiveHelper.load(2)
    val thresholdSettings = HiveHelper.load(1)
    val dstream = ssc.receiverStream(new PCap4jReceiver())

    val specifiedIds = checkSpecifiedIps(limitSettings, thresholdSettings, dstream)
    checkUpspecifiedIps(limitSettings, thresholdSettings, specifiedIds, dstream)

    TrafficAnalyzer.hourlyStatistics(dstream)

    ssc.start()
    ssc.awaitTermination()
  }

  def checkSpecifiedIps(limitSettings: scala.collection.Map[String, Settings],
                        thresholdSettings: scala.collection.Map[String, Settings],
                        dstream: DStream[NetworkPacket]): (mutable.MutableList[String], mutable.MutableList[String]) = {
    var specifiedLimitIps = mutable.MutableList[String]()
    limitSettings.foreach {
      e => {
        val ip = e._1
        val period = e._2.period
        val limit = e._2.value

        TrafficAnalyzer.checkForLimit(dstream, (packet: NetworkPacket) => packet.ip.equals(ip), Minutes(period),
          limit)
        specifiedLimitIps += ip
      }
    }

    var specifiedThrshIps = mutable.MutableList[String]()
    thresholdSettings.foreach {
      e => {
        val ip = e._1
        val period = e._2.period
        val limit = e._2.value

        TrafficAnalyzer.checkForThreshold(dstream, (packet: NetworkPacket) => packet.ip.equals(ip), Minutes(period),
          limit)
        specifiedThrshIps += ip
      }
    }

    (specifiedLimitIps, specifiedThrshIps)
  }

  def checkUpspecifiedIps(limitSettings: scala.collection.Map[String, Settings],
                          thresholdSettings: scala.collection.Map[String, Settings],
                          specifiedIds: (mutable.MutableList[String], mutable.MutableList[String]),
                          dstream: DStream[NetworkPacket]): Unit = {
    val defaultLimSettings = limitSettings.get("NULL").get
    val defaultThSettings = thresholdSettings.get("NULL").get

    TrafficAnalyzer.checkForLimit(dstream, (packet: NetworkPacket) => !specifiedIds._1.contains(packet.ip),
      Minutes(defaultLimSettings.period), defaultLimSettings.value)
    TrafficAnalyzer.checkForThreshold(dstream, (packet: NetworkPacket) => !specifiedIds._2.contains(packet.ip),
      Minutes(defaultThSettings.period), defaultThSettings.value)
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
