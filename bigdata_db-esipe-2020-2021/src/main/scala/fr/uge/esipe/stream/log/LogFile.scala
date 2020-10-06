package fr.uge.esipe.stream.log

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import scala.util.Using

/**
  * A log file manages a file made of offset and key-value data in
  * binary format. It has to be paired with an [[IndexFile]] instance.
  * Each instance is bounded to a partition.
  *
  * A log record in the file has the following format:
  * {{{
  * Header
  * +-----------------+-----------------+-----------------+
  * | offset (4B)     | key size (4B)   | value size (4B) |
  * +-----------------+-----------------+-----------------+
  * Data
  * +----------------------+------------------------------+
  * | key (byte array)     | value (byte array)           |
  * +----------------------+------------------------------+
  * }}}
  *
  * Thus, the header size is 12B. The data size depends on the values
  * for key size and value size.
  *
  * @param file file path where the log file is
  */
class LogFile(file: Path) {
  val headerSize: Int =
    (4     // offset
      + 4  // key size
      + 4) // value size

  /**
    * Get the log at a given position.
    *
    * WARN a position not aligned with stored logs will result in unpredictable data.
    * TODO a CRC might be added to the log in a view to check if the position is correct
    *
    * @param position position in bytes from the beginning of the log file
    * @return a log record
    */
  def getLog(position: Long): LogRecord = {
    Using(FileChannel.open(file, StandardOpenOption.READ)) { f =>
      f.position(position)
      LogRecord.read(f)
    }.get
  }

  /**
    * Create a log from data and add it in the log file.
    *
    * @param record data to add in the new log
    * @param offset offset of the log to add
    * @return position in the log file where the data has been added
    */
  def append(record: ProducerRecord, offset: Int): Long = {
    Using(FileChannel.open(file, StandardOpenOption.APPEND)) { f =>
      val position = f.position()
      val log      = LogRecord(offset, record.key, record.value)
      val buffer   = log.createBuffer()
      log.write(buffer)
      buffer.rewind()

      f.write(buffer)

      position
    }.get
  }
}

case class LogRecord(offset: Int, key: Array[Byte], value: Array[Byte]) {
  val size: Int = LogRecord.headerSize + key.length + value.length

  def createBuffer(): ByteBuffer = ByteBuffer.allocate(size)

  def write(buffer: ByteBuffer): Unit = {
    buffer.putInt(offset)
    buffer.putInt(key.length)
    buffer.putInt(value.length)
    buffer.put(key)
    buffer.put(value)
  }
}
object LogRecord {
  val headerSize: Int =
    (4     // offset
      + 4  // key size
      + 4) // value size

  def read(channel: FileChannel): LogRecord = {
    val headerBuffer = ByteBuffer.allocate(headerSize)
    channel.read(headerBuffer)
    headerBuffer.rewind()

    val offset    = headerBuffer.getInt()
    val keySize   = headerBuffer.getInt()
    val valueSize = headerBuffer.getInt()

    val keyBuffer   = ByteBuffer.allocate(keySize)
    val valueBuffer = ByteBuffer.allocate(valueSize)
    channel.read(keyBuffer)
    channel.read(valueBuffer)
    keyBuffer.rewind()
    valueBuffer.rewind()

    LogRecord(offset, keyBuffer.array(), valueBuffer.array())
  }
}
