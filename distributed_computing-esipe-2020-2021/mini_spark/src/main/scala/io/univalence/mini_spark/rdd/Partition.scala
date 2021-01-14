package io.univalence.mini_spark.rdd

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer

/**
 * Represent the description of a partition
 */
sealed trait Partition {
  val id: String
}

/**
 * The description of a partition for a splitted (partitioned) file is
 * just the path to a file that represents this partition.
 */
case class SplitFilePartition(path: String) extends Partition {
  override val id: String = {
    ".*-part-([0-9]+).*".r
      .findFirstMatchIn(path)
      .map(_.group(1))
      .get
  }
}
object SplitFilePartition {
  def getPartitions(directory: String): List[SplitFilePartition] = {
    val dir = new File(directory)
    if (!dir.isDirectory)
      throw new IllegalArgumentException(s"$directory should be a directory")

    val filenames: List[String] =
      dir
        .list { (_: File, filename: String) =>
          filename.matches(".*-part-[0-9]+.*")
        }
        .toList
        .sortBy(identity)

    filenames.map(f => SplitFilePartition(dir.toPath.resolve(f).toString))
  }
}

/**
 * The description of a partition for a (non-partitioned) file is
 * composed of the path to this file, a starting offset of the
 * partition in the file, and the length in this file.
 */
case class FilePartition(
    path: String,
    startOffset: Long,
    length: Long,
    override val id: String
) extends Partition

object FilePartition {

  def getPartitions(path: String, partitionCount: Int): List[FilePartition] = {
    val fileSize            = Files.size(Paths.get(path))
    val avgSize             = fileSize / partitionCount.toDouble
    val targetPartitionSize = avgSize.ceil.toInt
    var remaining           = fileSize
    var startOffset         = 0L
    val partitions          = ArrayBuffer[FilePartition]()
    var partitionId         = 0

    while (remaining > targetPartitionSize) {
      val partition = FilePartition(path, startOffset, targetPartitionSize, f"$partitionId%04d")

      partitions += partition

      startOffset += targetPartitionSize
      remaining -= targetPartitionSize
      partitionId += 1
    }

    if (remaining > 0) {
      partitions += FilePartition(path, startOffset, remaining, f"$partitionId%04d")
    }

    partitions.toList
  }

}

/**
 * The partition for a Seq (or a List) is composed of the list itself
 * (it is supposed to fit the available memory), the starting index in
 * this list (startOffset), and the length of this partition.
 */
case class SeqPartition[A](
  seq: Seq[A],
  override val id: String,
  startOffset: Int,
  length: Int
) extends Partition {
  def isEmpty: Boolean = length == 0

  def data: Seq[A] =
    seq.slice(startOffset, (startOffset + length))
}
object SeqPartition {
  def getPartitions[A](
      seq: Seq[A],
      partitionCount: Int
                     ): List[SeqPartition[A]] = {
    val avgSize                  = seq.length / partitionCount.toDouble
    val targetPartitionSize: Int = avgSize.ceil.toInt
    var remaining                = seq.length
    var startOffset              = 0
    val partitions               = ArrayBuffer[SeqPartition[A]]()
    var partitionId: Int         = 0

    while (remaining > targetPartitionSize) {
      val partition = SeqPartition(seq, partitionId.toString, startOffset, targetPartitionSize)

      partitions += partition

      startOffset += targetPartitionSize
      remaining -= targetPartitionSize
      partitionId += 1
    }

    if (remaining > 0) {
      partitions += SeqPartition(seq, partitionId.toString, startOffset, remaining)
    }

    partitions.toList
  }
}

/**
 * The partition for a String is composed of the string itself (it is
 * supposed to fit the available memory), the starting index in this
 * string (startOffset), and the length of this partition.
 */
case class StringPartition(
    string: String,
    override val id: String,
    startOffset: Long,
    length: Long
) extends Partition {

  def isEmpty: Boolean = length == 0

  def data: String =
    string.substring(startOffset.toInt, (startOffset + length).toInt)

}

object StringPartition {
  def getPartitions(
      string: String,
      partitionCount: Int
  ): List[StringPartition] = {
    val avgSize                  = string.length / partitionCount.toDouble
    val targetPartitionSize: Int = avgSize.ceil.toInt
    var remaining                = string.length
    var startOffset              = 0
    val partitions               = ArrayBuffer[StringPartition]()
    var partitionId: Int         = 0

    while (remaining > targetPartitionSize) {
      val partition = StringPartition(string, partitionId.toString, startOffset, targetPartitionSize)

      partitions += partition

      startOffset += targetPartitionSize
      remaining -= targetPartitionSize
      partitionId += 1
    }

    if (remaining > 0) {
      partitions += StringPartition(string, partitionId.toString, startOffset, remaining)
    }

    partitions.toList
  }
}
