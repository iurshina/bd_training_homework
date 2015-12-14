name := "SparkHW2"

version := "1.0"

scalaVersion := "2.11.5"

mainClass in assembly := Some("Main")

libraryDependencies ++= Seq(
  "org.pcap4j" % "pcap4j-core" % "1.6.1",
  "org.pcap4j" % "pcap4j-packetfactory-static" % "1.6.1",
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.2",
  "org.apache.spark" % "spark-core_2.11" % "1.5.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "1.5.1" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.5.1" % "provided",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",
  "org.apache.spark" % "spark-hive_2.11" % "1.5.1" % "provided"
)
