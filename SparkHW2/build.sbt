name := "SparkHW2"

version := "1.0"

scalaVersion := "2.10.4"

mainClass in assembly := Some("Main")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.2" % "provided",
  "org.pcap4j" % "pcap4j-core" % "1.6.1",
  "org.pcap4j" % "pcap4j-packetfactory-static" % "1.6.1",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.0"
)