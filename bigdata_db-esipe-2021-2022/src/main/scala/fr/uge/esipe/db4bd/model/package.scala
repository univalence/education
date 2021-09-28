package fr.uge.esipe.db4bd

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

package object model {

  def serializeString(s: String): Array[Byte] = {
    val rawData = s.getBytes(StandardCharsets.UTF_8)
    val size    = rawData.length

    ByteBuffer
      .allocate(4 + size)
      .putInt(size)
      .put(rawData)
      .rewind()
      .array()
  }

  def serializeDouble(d: Double): Array[Byte] = {
    ByteBuffer
      .allocate(8)
      .putDouble(d)
      .rewind()
      .array()
  }

  def bin2string(bytes: Array[Byte]): String = {
    new String(bytes, StandardCharsets.UTF_8)
  }

  def buffer2string(buffer: ByteBuffer): String = {
    val size = buffer.getInt
    val data = Array.ofDim[Byte](size)
    buffer.get(data)

    new String(data, StandardCharsets.UTF_8)
  }

}
