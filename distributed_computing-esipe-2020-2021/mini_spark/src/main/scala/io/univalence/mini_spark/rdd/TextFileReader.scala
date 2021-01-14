package io.univalence.mini_spark.rdd

import java.io.{Closeable, InputStream}
import java.nio.charset.StandardCharsets

class TextFileReader(in: InputStream, bufferSize: Int) extends Closeable {
  import TextFileReader._

  val EndOfFile: (Int, String) = (-1, "")
  val buffer: Array[Byte]      = Array.ofDim[Byte](bufferSize)
  var totalByteRead: Long      = 0L;
  var bufferLength: Int        = 0
  var bufferPosition: Int      = 0

  def readLine(): (Int, String) = {
    val stringBuffer: StringBuffer = new StringBuffer()
    var totalRead: Int             = 0

    while (!hasEndOfLine_) {
      val (byteRead, data) = readChunck
      if (byteRead > 0) {
        totalRead += byteRead
        stringBuffer.append(data)
      }
    }

    if (bufferLength < 0 && totalRead == 0) {
      EndOfFile
    } else {
      if (bufferPosition < bufferLength && hasEndOfLine_) {
        bufferPosition += 1
      }

      (totalRead, stringBuffer.toString)
    }
  }

  def readChunck: (Int, String) = {
    var startPosition: Int = 0

    if (bufferPosition >= bufferLength) {
      bufferPosition = 0
      startPosition = 0

      bufferLength = fillBuffer(in, buffer)
    }
    if (bufferLength >= 0) {
      startPosition = bufferPosition
      val byteRead = advanceCursorTillEndOfLine()

      val chunck = new String(
        buffer,
        startPosition,
        byteRead,
        StandardCharsets.UTF_8
      )
      val lineFeedCount = if (bufferPosition < bufferLength) 1 else 0

      (byteRead + lineFeedCount, chunck)
    } else {
      EndOfFile
    }
  }

  def hasEndOfLine_ : Boolean =
    hasEndOfLine(buffer, bufferPosition, bufferLength)

  private def advanceCursorTillEndOfLine(): Int =
    if (bufferLength > 0) {
      val startPosition = bufferPosition
      while (bufferPosition < bufferLength && !hasEndOfLine_) {
        bufferPosition += 1
      }

      bufferPosition - startPosition
    } else {
      -1
    }

  private def fillBuffer(in: InputStream, buffer: Array[Byte]): Int = {
    val byteRead = in.read(buffer)

    if (byteRead > 0) totalByteRead += byteRead

    byteRead
  }

  override def close(): Unit = in.close()

}

object TextFileReader {

  def hasEndOfLine(buffer: Array[Byte], position: Int, length: Int): Boolean =
    length < 0 || (position < length && buffer(position) == '\n')

}
