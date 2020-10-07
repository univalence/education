package fr.uge.esipe.kafka

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.Using

object ProducerMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    Using(
      new KafkaProducer[String, String](
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
        ).asJava,
        new StringSerializer,
        new StringSerializer
      )
    ) { producer =>
      val record = new ProducerRecord[String, String](
        "my-topic",
        "key42",
        "hello42"
      )

      val metadata = producer.send(record).get()
      println(metadata)
    }.get
  }
}
