package org.example.hotitems.analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {
  def main(arg: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092")
    //properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //properties.setProperty("auto.offset.reset", "latest")

    val producer = new KafkaProducer[String, String](properties)

    val bufferSource = io.Source.fromFile("/Users/hliu/hao/flinkexample/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    for (line <- bufferSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
