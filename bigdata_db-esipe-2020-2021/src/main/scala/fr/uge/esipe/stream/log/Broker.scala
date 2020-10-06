package fr.uge.esipe.stream.log

import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

class Broker(workDir: Path) {
  val checkPointFile: CheckPointFile = {
    val file = workDir.resolve("checkpoint")
    if (!file.toFile.exists()) Files.createFile(file)

    new CheckPointFile(file)
  }

  val topics: mutable.Map[String, List[Partition]] = mutable.Map.empty

  locally {
    val partitions =
      checkPointFile
        .getOffets()
        .keys
        .groupBy(_.topic)
        .view
        .mapValues(_.size)

    for ((t, pc) <- partitions) {
      createTopic(t, pc)
    }
  }

  def hasTopic(topic: String): Boolean = topics.contains(topic)

  def createTopic(name: String, partitionCount: Int): Unit = {
    val partitions: List[Partition] =
      List.tabulate(partitionCount)(i => createPartition(name, i))

    topics += (name -> partitions)
  }

  private def createPartition(topic: String, partitionId: Int): Partition = {
    val dir = workDir.resolve(s"$topic-$partitionId")

    if (!dir.toFile.exists()) {
      Files.createDirectories(dir)
      checkPointFile.setOffset(topic, partitionId, 0)
    }

    val lastOffset = checkPointFile.getOffset(topic, partitionId).get

    new Partition(partitionId, dir, lastOffset)
  }

  def send(record: ProducerRecord): (Int, Int) = {
    (for (partitions <- topics.get(record.topic))
      yield {
        val partitionId =
          MurmurHash3.bytesHash(record.key).abs % partitions.size
        val partition = topics(record.topic)(partitionId)

        val offset =
          checkPointFile.getOffset(record.topic, partitionId).getOrElse(0)
        val newOffset =
          partition.append(record, offset)

        checkPointFile.setOffset(record.topic, partitionId, newOffset)

        (partitionId, offset)
      }).getOrElse(throw new IllegalArgumentException(record.toString))
  }

  def poll(
      topic: String,
      partitionId: Int,
      offset: Int
  ): Seq[ConsumerRecord] = {
    for {
      partitions <- topics.get(topic)
      partition  <- partitions.lift(partitionId)
    } yield {
      for ((key, value) <- partition.poll(offset))
        yield ConsumerRecord(topic, partitionId, offset, key, value)
    }
  }.get
}
