package org.yurshina

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

case class AlertMessage(ip: String, actualValue: Double, limit: Double, period: Long) {

  override def toString: String = {
    s"(${System.currentTimeMillis() / 1000}) Ip: $ip, Bytes: $actualValue, Limit (bytes): $limit, Period (sec): $period"
  }
}

object KafkaPublisher {

  val TOPIC = "alerts"

  val properties = new java.util.HashMap[String, Object]() {
    {
      put("zk.connect", "localhost:2181")
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    }
  }

  val producer = new KafkaProducer[String, String](properties)

  def send(message: AlertMessage) {
    producer.send(new ProducerRecord[String, String](TOPIC, null, message.toString))
  }
}

