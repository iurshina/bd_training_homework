name := "SparkHW"

version := "1.0"

scalaVersion := "2.11.7"

mainClass in assembly := Some("Main")

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.5.1" % "provided"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.5" % "test"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
