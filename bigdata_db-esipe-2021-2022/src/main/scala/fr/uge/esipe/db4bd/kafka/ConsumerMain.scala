package fr.uge.esipe.db4bd.kafka

import java.time.Duration
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.streams.scala.serialization.Serdes

object ConsumerMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val consumer = new KafkaConsumer[String, String](
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> "group-1",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ).asJava,
      Serdes.stringSerde.deserializer(),
      Serdes.stringSerde.deserializer()
    )

    try {
      consumer.subscribe(List("my_topic").asJava)

      val records: ConsumerRecords[String, String] =
        consumer.poll(Duration.ofSeconds(5))

      if (!records.isEmpty) {
        records.asScala.foreach(record =>
          println(record)
        )
      } else {
        println("no nore message")
      }
    } finally {
      consumer.close()
    }
  }
}
