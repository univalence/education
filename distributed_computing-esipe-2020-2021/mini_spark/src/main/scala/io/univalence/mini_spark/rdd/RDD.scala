package io.univalence.mini_spark.rdd

import io.univalence.mini_spark.computation.{JobContext, LocalJobContext, TaskResult}
import java.io.{Closeable, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.language.implicitConversions
import scala.util.Using

sealed trait RDD[A] {
  val partitions: Seq[Partition]
  val jobContext: JobContext

  def compute(partition: Partition): CloseableIterator[A]

  def map[B](f: A => B): RDD[B] =
    RDD.MapRDD(this, f)

  def filter(p: A => Boolean): RDD[A] =
    RDD.FilterRDD(this, p)

  def groupBy[K](key: A => K): RDD[(K, Seq[A])] =
    RDD.GroupByRDD(this, key)

  def collect: Seq[A] =
    jobContext.runJob(this)

  def count: Int =
    jobContext.runJob(this, (s: Seq[TaskResult[A]]) => s.map(_.data.length).sum)

  def saveTextFile(filename: String): Unit = {
    def leftPad(s: String, n: Int, c: Char): String =
      c.toString * Math.max(0, n - s.length) + s

    val path = Paths.get(filename)
    val dir  = path.getParent
    if (!dir.toFile.exists())
      Files.createDirectories(dir)

    val m        = "^([^.]+)(\\.[^.]*)?$".r.findFirstMatchIn(filename).get
    val basename = m.group(1)
    val ext      = Option(m.group(2)).getOrElse("")

    jobContext.runJob(
      this,
      { (results: Seq[TaskResult[A]]) =>
        results.map {
          result =>
            val partitionId = result.context.partition.id
            val partFilename =
              f"$basename%s-part-${leftPad(partitionId, 5, '0')}$ext"

            val wholeData = result.data
              .map(_.toString)
              .mkString("\n")

            Using(Files.newOutputStream(Paths.get(partFilename))) { file =>
              file.write(wholeData.getBytes(StandardCharsets.UTF_8))
            }.get
        }

      }
    )
  }

  def toLocal: RDD[A]

}

object RDD {
  import CloseableIterator.implicits._

  case class MapRDD[A, B](prev: RDD[A], f: A => B) extends RDD[B] {
    override val partitions: Seq[Partition] = prev.partitions
    override val jobContext: JobContext     = prev.jobContext

    override def compute(partition: Partition): CloseableIterator[B] =
      prev.compute(partition).map(f)

    override def toLocal: RDD[B] = MapRDD(prev.toLocal, f)
  }

  case class FilterRDD[A](prev: RDD[A], p: A => Boolean) extends RDD[A] {
    override val partitions: Seq[Partition] = prev.partitions
    override val jobContext: JobContext     = prev.jobContext

    override def compute(partition: Partition): CloseableIterator[A] =
      prev.compute(partition).filter(p)

    override def toLocal: RDD[A] = FilterRDD(prev.toLocal, p)
  }

  case class GroupByRDD[K, V](prev: RDD[V], key: V => K)
      extends RDD[(K, Seq[V])] {
    override val partitions: Seq[Partition] = prev.partitions
    override val jobContext: JobContext     = prev.jobContext

    override def compute(partition: Partition): CloseableIterator[(K, Seq[V])] =
      prev
        .compute(partition)
        .toSeq
        .groupBy(key)
        .iterator

    override def toLocal: RDD[(K, Seq[V])] = GroupByRDD(prev.toLocal, key)
  }

  case class StringRDD(
                        string: String,
                        partitionCount: Int,
                        override val jobContext: JobContext
                      ) extends RDD[String] {
    override val partitions: Seq[Partition] = StringPartition.getPartitions(string, partitionCount)

    override def compute(split: Partition): CloseableIterator[String] = {
      new CloseableIterator[String] {
        val stringSplit: SeqPartition[String] = split.asInstanceOf[SeqPartition[String]]
        val iter: Iterator[String] = stringSplit.data.iterator

        override def hasNext: Boolean = iter.hasNext

        override def next(): String = iter.next()

        override def close(): Unit = iter.close()
      }
    }


    override def toLocal: RDD[String] = copy(jobContext = new LocalJobContext)
  }

  case class SeqRDD[A](
                        seq: Seq[A],
                        partitionCount: Int,
                        override val jobContext: JobContext
                      ) extends RDD[A] {
    override val partitions: Seq[Partition] = SeqPartition.getPartitions(seq, partitionCount)

    override def compute(split: Partition): CloseableIterator[A] = {
      new CloseableIterator[A] {
        val seqSplit: SeqPartition[A] = split.asInstanceOf[SeqPartition[A]]
        val iter: Iterator[A] = seqSplit.data.iterator

        override def hasNext: Boolean = iter.hasNext

        override def next(): A = iter.next()

        override def close(): Unit = iter.close()
      }
    }


    override def toLocal: RDD[A] = copy(jobContext = new LocalJobContext)
  }

  case class TextFileRDD(
      filepath: String,
      partitionCount: Int,
      override val jobContext: JobContext
  ) extends RDD[String] {
    override val partitions: Seq[Partition] = {
      FilePartition.getPartitions(filepath, partitionCount)
    }

    override def compute(split: Partition): CloseableIterator[String] = {
      new CloseableIterator[String] {
        val fileSplit: FilePartition = split.asInstanceOf[FilePartition]
        val file                     = new FileInputStream(filepath)
        val splitFileReaderIterator: SplitFileReaderIterator =
          new SplitFileReaderIterator(
            file,
            fileSplit.startOffset,
            fileSplit.length
          )

        override def hasNext: Boolean = splitFileReaderIterator.hasNext

        override def next(): String = splitFileReaderIterator.next()

        override def close(): Unit = file.close()
      }
    }

    override def toLocal: RDD[String] = copy(jobContext = new LocalJobContext)
  }

  case class SplitTextFileRDD(
      filepath: String,
      override val jobContext: JobContext
  ) extends RDD[String] {
    override val partitions: Seq[Partition] = {
      SplitFilePartition.getPartitions(filepath)
    }

    override def compute(split: Partition): CloseableIterator[String] = {
      new CloseableIterator[String] {
        val fileSplit: SplitFilePartition =
          split.asInstanceOf[SplitFilePartition]
        val file: Iterator[String] = Source.fromFile(fileSplit.path).getLines()

        override def hasNext: Boolean = file.hasNext

        override def next(): String = file.next()

        override def close(): Unit = file.close()
      }
    }

    override def toLocal: RDD[String] = copy(jobContext = new LocalJobContext)
  }

}

trait CloseableIterator[A] extends Iterator[A] with Closeable
object CloseableIterator {
  object implicits {

    implicit def toCloseableIterator[A](i: Iterator[A]): CloseableIterator[A] =
      new CloseableIterator[A] {
        override def hasNext: Boolean = i.hasNext

        override def next(): A = i.next()

        override def close(): Unit = {}
      }

  }
}