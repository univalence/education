package fr.uge.esipe.db4bd.model

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}
import scala.util.Try

case class Checkin(userId: String, venueId: String, timestamp: LocalDateTime, offset: Int) {
  def serialize: Array[Byte] = {
    val userIdBin = serializeString(userId)
    val venueIdBin = serializeString(venueId)
    val ts = timestamp.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli

    val buffer = ByteBuffer.allocate(userIdBin.length + venueIdBin.length + 8 + 4)
    buffer
      .put(userIdBin)
      .put(venueIdBin)
      .putLong(ts)
      .putInt(offset)

    buffer.rewind().array()
  }
}
object Checkin {
  val datePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("EEE MMM dd kk:mm:ss Z yyyy")

  def deserialize(buffer: ByteBuffer): Checkin = {
    val userId = buffer2string(buffer)
    val venueId = buffer2string(buffer)
    val ts = buffer.getLong()
    val timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault())
    val offset = buffer.getInt

    Checkin(userId, venueId, timestamp, offset)
  }
}