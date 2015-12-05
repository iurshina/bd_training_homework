package org.yurshina.flights

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Flights counter")

    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)

    val flightsDao = new FlightsDao(sqlc)

    val year2007 = loadCsvFile("/home/nastya/java/Big data training/2007.csv", sqlc).cache()
    year2007.registerTempTable("year_2007")

    val carriers = loadCsvFile("/home/nastya/java/Big data training/carriers.csv", sqlc).cache()
    carriers.registerTempTable("Carriers")

    val airports = loadCsvFile("/home/nastya/java/Big data training/airports.csv", sqlc).cache()
    airports.registerTempTable("Airports")

    flightsDao.numberOfFlights().collect().foreach(println(_))

    println(flightsDao.numberOfFlightsJuneNYC())

    flightsDao.mostBusyAirportsJunAug().collect().foreach(println(_))

    println(flightsDao.carrierLargestNumerOfFlights())
  }

  def loadCsvFile(path: String, sqlContext: SQLContext): DataFrame = {
    val result = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    result
  }
}
