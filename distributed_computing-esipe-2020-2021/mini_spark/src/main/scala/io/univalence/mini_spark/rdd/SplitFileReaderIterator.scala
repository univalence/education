package io.univalence.mini_spark.rdd

import java.io.InputStream

class SplitFileReaderIterator(in: InputStream, start: Long, targetLength: Long)
    extends Iterator[String] {
  val reader            = new TextFileReader(in, 64 * 1024)
  var lastByteRead: Int = 0
  var totalRead: Long   = 0L
  var startAt: Long     = 0L

  locally {
    if (start > 0) {
      in.skip(start)
      val (byteRead, _) = reader.readLine()
      if (byteRead < 0) lastByteRead = -1
      else {
        totalRead = byteRead
        startAt = start + byteRead
      }
    }
  }

  override def hasNext: Boolean = {
    lastByteRead >= 0 && totalRead < targetLength
  }

  override def next(): String = {
    if (!hasNext) throw new NoSuchElementException

    val (byteRead, data) = reader.readLine()

    lastByteRead = byteRead
    if (byteRead > 0) {
      totalRead += byteRead
      startAt += byteRead
    } else {
      throw new NoSuchElementException(s"end of file")
    }
    data
  }
}
