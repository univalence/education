package fr.uge.esipe.db4bd.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.streams.scala.serialization.Serdes

object ProducerMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val producer = new KafkaProducer[String, String](
      Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
      ).asJava,
      Serdes.stringSerde.serializer(),
      Serdes.stringSerde.serializer()
    )

    try {
      for (i <- 1 to 10) {
        val record = new ProducerRecord[String, String](
          "my_topic",
          "key1",
          s"value$i"
        )

        val result: RecordMetadata = producer.send(record).get()
        println(result)
      }
    } finally {
      producer.close()
    }
  }
}
