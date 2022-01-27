package fr.universite_gustave_eiffel.esipe.spark_course.response

import java.io.File

object TextFilePartitioningMain {

  def main(args: Array[String]): Unit = {
    val fileSize       = 100 * 1024 * 1024
    val filename       = "my-file.data"
    val partitionCount = 9

    val file = new File(filename)

    // TESTS
    println(partition(5, file, 1))
    println(partition(5, file, 2))

    println("Result of the exercise:")
    println(s"size = $fileSize")
    partition(fileSize, file, partitionCount).foreach(println)
  }

  def partition(size: Long, file: File, partitionCount: Int): List[FilePartition] = {
    val partitionSize = size / partitionCount

    val firstPartitions: List[FilePartition] =
      (0.until(partitionCount - 1))
        .map { partitionId =>
          FilePartition(
            id          = partitionId,
            file        = file,
            startOffset = partitionSize * partitionId,
            length      = partitionSize
          )
        }
        .toList

    val lastPartition: FilePartition =
      FilePartition(
        id          = partitionCount - 1,
        file        = file,
        startOffset = partitionSize * (partitionCount - 1),
        length      = size - (partitionSize * (partitionCount - 1))
      )

    firstPartitions :+ lastPartition
  }

}

case class FilePartition(id: Int, file: File, startOffset: Long, length: Long)
