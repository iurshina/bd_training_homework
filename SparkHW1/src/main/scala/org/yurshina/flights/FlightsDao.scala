package org.yurshina.flights

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class FlightsDao(sqlc: SQLContext) {

  def numberOfFlights(): RDD[(Long, String)] = sqlc
    .sql("select count(*) as NumberOfFlights, Carriers.description as carrier " +
      "from year_2007 join Carriers on year_2007.UniqueCarrier=Carriers.code " +
      "group by Carriers.description")
    .map(row => (
      row.getLong(0),
      row.getString(1)
      ))

  def numberOfFlightsJuneNYC(): Long = sqlc
    .sql("select count(*) as NumberOfFlights " +
      "from year_2007 " +
      "join Airports as origin on year_2007.origin=origin.iata " +
      "join Airports as dest on year_2007.dest=dest.iata " +
      "where (origin.city ='New York' or dest.city='New York') and year_2007.month=6")
    .map(row => row.getLong(0)).first()

  def mostBusyAirportsJunAug(): RDD[(Long, String)] = sqlc
    .sql("select sum(busiest.flightcount) as sum, busiest.airport from " +
      "(select count(d.FlightNum) as flightcount, a.airport from year_2007 d join Airports a on a.iata = d.Dest " +
      "where d.Month between 6 and 8 group by a.airport " +
      "UNION ALL " +
      "select count(d.FlightNum) as flightcount, a1.airport from year_2007 d join Airports a1 on a1.iata = d.Origin " +
      "where d.Month between 6 and 8 group by a1.airport) " +
      "as busiest group by busiest.airport sort by sum desc limit 5")
    .map(row => (
      row.getLong(0),
      row.getString(1)
      ))

  def carrierLargestNumerOfFlights(): (Long, String) = sqlc
    .sql("select count(*) as NumberOfFlights, Carriers.description as carrier " +
      "from year_2007 join Carriers on year_2007.UniqueCarrier=Carriers.code " +
      "group by Carriers.description " +
      "order by NumberOfFlights desc limit 1")
    .map(row => (row.getLong(0), row.getString(1))).first()
}
