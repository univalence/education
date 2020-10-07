package fr.uge.esipe.stream.log

import java.nio.file.{Files, Paths}
import java.util.UUID

object LogMain {
  def main(args: Array[String]): Unit = {
    val workDir = Paths.get("stream")
    if (!workDir.toFile.exists()) Files.createDirectories(workDir)

    val broker: Broker = new Broker(workDir)

    broker.createTopic("my-topic", 3)

//        produce(broker)
    consume(broker)
  }

  def consume(broker: Broker): Unit = {
    val records = broker.poll(
      topic = "my-topic",
      partitionId = 2,
      offset = 0
    )

    for (record <- records) {
      val key   = new String(record.key)
      val value = new String(record.value)

      println(s"$key = $value")
    }
  }

  def produce(broker: Broker): Unit = {
    for (i <- 1 to 100) {
      val key = s"key${i % 10}"

      val record = ProducerRecord(
        topic = "my-topic",
        key = key.getBytes(),
        value = UUID.randomUUID().toString.getBytes()
      )

      broker.send(record)
    }
  }
}
