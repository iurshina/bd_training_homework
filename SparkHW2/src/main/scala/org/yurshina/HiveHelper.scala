package org.yurshina

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.yurshina.TrafficAnalyzer.Statistics

object HiveHelper {

  case class Settings(value: Double, period: Long)

  def load(eventType: Int)(implicit hiveContext: HiveContext): scala.collection.Map[String, Settings] = {
    val settings = readCsvFile("/home/nastya/IdeaProjects/SparkHW2/src/main/resources/conf.csv", hiveContext)
      .cache()

    settings.filter(settings("Type") === eventType)
      .map(df => (df(0).toString, Settings(df(2).toString.toDouble, df(3).toString.toLong)))
      .collectAsMap()
  }

  def readCsvFile(path: String, sqlContext: SQLContext): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }

  def insertHourlyStats(dstream: DStream[Statistics])(implicit hiveContext: HiveContext): Unit = {
    dstream.foreachRDD(rdd => {
      val schemaString = "ip,totalSize,speedBytesPerSec"
      val schema = StructType(schemaString.split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true)))
      val rowRDD = rdd.map(p => Row(p.ip, p.totalSize, p.speedBytesPerSec))

      val personSchemaRDD = hiveContext.createDataFrame(rowRDD, schema)
      personSchemaRDD.write.insertInto("hourly_stat")
    })
  }
}
