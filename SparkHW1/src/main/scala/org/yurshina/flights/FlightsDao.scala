package org.yurshina.flights

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class FlightsDao(sqlc: SQLContext) {

  def numberOfFlights(): RDD[(Long, String)] = sqlc
    .sql("select count(*) as NumberOfFlights, Carriers.description as carrier from year_2007 join Carriers on year_2007.UniqueCarrier=Carriers.code group by Carriers.description")
    .map(row => (
      row.getLong(0),
      row.getString(1)
      ))
}
