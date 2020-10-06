package fr.uge.esipe.stream.log

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, StandardOpenOption}
import scala.collection.{MapView, mutable}
import scala.io.Source
import scala.util.Using

class CheckPointFile(file: Path) {
  private val checkpoints: mutable.Map[TopicPartition, Int] =
    mutable.Map.from(readContent())

  def getOffets(): MapView[TopicPartition, Int] = checkpoints.view

  def getOffset(topic: String, partition: Int): Option[Int] =
    checkpoints.get(TopicPartition(topic, partition))

  def setOffset(topic: String, partition: Int, offset: Int): Unit = {
    checkpoints(TopicPartition(topic, partition)) = offset
    writeContent()
  }

  private def readContent(): Map[TopicPartition, Int] =
    Using(Source.fromFile(file.toFile)) { f =>
      (for {
        line <- f.getLines()
        if !line.trim.isEmpty
      } yield {
        val fields = line.split(",", 3)
        TopicPartition(fields(0), fields(1).toInt) -> fields(2).toInt
      }).toMap
    }.get

  private def writeContent(): Unit =
    Using(FileChannel.open(file, StandardOpenOption.WRITE)) { channel =>
      for ((tp, offset) <- checkpoints) {
        val line   = s"${tp.topic},${tp.partitionId},$offset\n"
        val buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8))
        channel.write(buffer)
      }
    }.get
}

case class TopicPartition(topic: String, partitionId: Int)
