package fr.uge.esipe.db4bd.test_kakfa

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.Serdes

object MyTopicConsumerMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val consumer =
    new KafkaConsumer[String, String](
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> s"${getClass.getCanonicalName}",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ).asJava,
      Serdes.String().deserializer(),
      Serdes.String().deserializer()
    )

    consumer.subscribe(List("my-topic").asJava)

    val records = consumer.poll(java.time.Duration.ofMinutes(10))

    records.asScala
      .foreach(println)
  }
}
