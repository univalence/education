package fr.uge.esipe.stream.log

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, StandardOpenOption}
import scala.collection.mutable
import scala.io.Source
import scala.util.Using

/**
 * A index file is a readable sequence of offset and position in a log
 * file.
 *
 * @param file index file path
 */
class IndexFile(file: Path) {
  // ArrayBuffer here keeps the addition order
  private val indexes: mutable.ArrayBuffer[(Int, Long)] =
    mutable.ArrayBuffer.from(readIndexes())

  def getPosition(offset: Int): Option[Long] =
    indexes
      .find(_._1 == offset)
      .map(_._2)

  def append(offset: Int, position: Long): Unit = {
    indexes += (offset -> position)
    writeIndexes()
  }

  private def readIndexes(): List[(Int, Long)] =
    Using(Source.fromFile(file.toFile)) { f =>
      (for (line <- f.getLines())
        yield {
          val fields = line.split(",", 2)
          fields(0).toInt -> fields(1).toLong
        }).toList
    }.get

  private def writeIndexes(): Unit =
    Using(FileChannel.open(file, StandardOpenOption.WRITE)) { channel =>
      for ((offset, position) <- indexes) {
        val line   = s"$offset,$position\n"
        val buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8))
        channel.write(buffer)
      }
    }.get
}
