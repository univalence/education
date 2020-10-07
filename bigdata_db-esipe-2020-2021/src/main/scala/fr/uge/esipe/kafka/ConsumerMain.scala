package fr.uge.esipe.kafka

import java.time.Duration
import java.util.UUID
import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRecords,
  KafkaConsumer
}
import org.apache.kafka.common.serialization.StringDeserializer
import scala.util.Using

object ConsumerMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    Using(
      new KafkaConsumer[String, String](
        Map[String, AnyRef](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
          ConsumerConfig.GROUP_ID_CONFIG          -> "group2",
//          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        ).asJava,
        new StringDeserializer,
        new StringDeserializer
      )
    ) { consumer =>
      consumer.subscribe(List("my-topic").asJava)

      val records: ConsumerRecords[String, String] =
        consumer.poll(Duration.ofMinutes(60))

      for (record <- records.asScala) {
        println(s"${record.key()} -> ${record.value()}")
      }
    }
  }
}
