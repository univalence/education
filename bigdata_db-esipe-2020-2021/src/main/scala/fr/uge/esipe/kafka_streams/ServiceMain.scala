package fr.uge.esipe.kafka_streams

import java.util.{Properties, UUID}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, KTable, Materialized, Produced}

object ServiceMain {
  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    val stream: KStream[String, String] =
      builder.stream("inputstream")(
        Consumed.`with`(Serdes.String, Serdes.String)
      )

    val table: KTable[String, Long] =
      stream
        .groupByKey(Grouped.`with`(Serdes.String, Serdes.String))
        .count()(Materialized.`with`(Serdes.String, Serdes.Long))

    table
      .toStream
      .to("outputstream")(Produced.`with`(Serdes.String, Serdes.Long))

    val service =
      new KafkaStreams(
        builder.build(),
        JavaProperties(
          StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
          StreamsConfig.APPLICATION_ID_CONFIG    -> s"ServiceMain-${UUID.randomUUID().toString}"
        )
      )

    Runtime.getRuntime.addShutdownHook(new Thread(() => service.close()))
    service.start()
  }
}

object JavaProperties {
  def apply(props: (String, AnyRef)*): Properties = {
    val p = new Properties()

    for ((k, v) <- props) {
      p.put(k, v)
    }

    p
  }
}
