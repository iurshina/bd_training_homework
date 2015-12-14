package org.yurshina

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Minutes}

object TrafficAnalyzer {

  case class State(prevValue: Double, curValue: Double)

  def checkForLimit(dstream: DStream[NetworkPacket],
                    packetFilter: NetworkPacket => Boolean,
                    timePeriod: Duration,
                    limit: Double): Unit = {
    dstream
      .filter(packetFilter)
      .map(packet => (packet.ip, packet.lengthBytes))
      .window(timePeriod, timePeriod)
      .reduceByKey(_ + _)
      .updateStateByKey((bytes: Seq[Int], state: Option[State]) => {
        val totalBytes = bytes.sum
        val curState = state.getOrElse(State(0, 0))

        Some(State(curState.curValue, totalBytes))
      })
      .foreachRDD(rdd => {
        rdd.collect().foreach(ipAndState => {
          val ip = ipAndState._1
          val state = ipAndState._2

          if (state.curValue > limit && state.prevValue <= limit) {
            println(s"Report limit exceed: $ip")
            doReport(ip, state.curValue, timePeriod, limit)
          } else if (state.curValue <= limit && state.prevValue > limit) {
            println(s"Report limit normalization: $ip")
          }
        })
      })
  }

  def checkForThreshold(dstream: DStream[NetworkPacket],
                        packetFilter: NetworkPacket => Boolean,
                        timePeriod: Duration,
                        threshold: Double): Unit = {
    dstream
      .filter(packetFilter)
      .map(packet => (packet.ip, packet.lengthBytes))
      .window(timePeriod, timePeriod)
      .reduceByKey(_ + _)
      .updateStateByKey((bytes: Seq[Int], state: Option[State]) => {
        val speed = bytes.sum / (timePeriod.milliseconds / 1000)
        val curState = state.getOrElse(State(0, 0))

        Some(State(curState.curValue, speed))
      })
      .foreachRDD(rdd => {
        rdd.collect().foreach(ipAndState => {
          val ip = ipAndState._1
          val state = ipAndState._2

          if (state.curValue > threshold && state.prevValue <= threshold) {
            println(s"Report threshold exceed: $ip")
            doReport(ip, state.curValue, timePeriod, threshold)
          } else if (state.curValue <= threshold && state.prevValue > threshold) {
            println(s"Report threshold normalization: $ip")
          }
        })
      })
  }

  def doReport(ip: String,
               bytes: Double,
               timePeriod: Duration,
               limit: Double): Unit = {
    KafkaPublisher.send(AlertMessage(ip, bytes, limit, timePeriod.milliseconds))
  }

  case class Statistics(ip: String, totalSize: String, speedBytesPerSec: String)

  def hourlyStatistics(dstream: DStream[NetworkPacket])(implicit hiveContext: HiveContext): Unit = {
    val hour = Minutes(60)

    val stats = dstream.map(packet => (packet.ip, packet.lengthBytes))
      .window(hour, hour)
      .reduceByKey(_ + _)
      .map(rdd => {
        val ip = rdd._1
        val totalSize = rdd._2
        val speedBytesPerSec: Double = totalSize / 3600d

        println(s"Ip: $ip, totalSize: $totalSize, speedBytesPerSec: $speedBytesPerSec")
        Statistics(ip, totalSize.toString, speedBytesPerSec.toString)
      })

    HiveHelper.insertHourlyStats(stats)
  }
}
