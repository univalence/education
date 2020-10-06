package fr.uge.esipe.stream.log

import java.nio.file.{Files, Path}

class Partition(val partitionId: Int, workDir: Path, checkpointOffset: Int) {
  val logFilename   = "partition.log"
  val indexFilename = "partition.idx"

  val indexFile: IndexFile = {
    val file = workDir.resolve(indexFilename)
    if (!file.toFile.exists()) Files.createFile(file)

    new IndexFile(file)
  }

  val logFile: LogFile = {
    val file = workDir.resolve(logFilename)
    if (!file.toFile.exists()) Files.createFile(file)

    new LogFile(file)
  }

  var maxOffset: Int = checkpointOffset

  def append(record: ProducerRecord, offset: Int): Int = {
    val position = logFile.append(record, offset)
    indexFile.append(offset, position)

    maxOffset += 1
    offset + 1
  }

  def poll(offset: Int): Seq[(Array[Byte], Array[Byte])] = {
    for (current <- offset until maxOffset) yield {
      val position = indexFile.getPosition(current).get
      val log      = logFile.getLog(position)

      (log.key, log.value)
    }
  }
}
