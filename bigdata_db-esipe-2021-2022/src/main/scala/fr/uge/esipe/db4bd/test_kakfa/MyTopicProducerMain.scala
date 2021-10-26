package fr.uge.esipe.db4bd.test_kakfa

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes

object MyTopicProducerMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val producer =
      new KafkaProducer[String, String](
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
        ).asJava,
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )

    val record = new ProducerRecord[String, String]("my-topic", "key1", "hello")
    producer.send(record).get()
  }
}
