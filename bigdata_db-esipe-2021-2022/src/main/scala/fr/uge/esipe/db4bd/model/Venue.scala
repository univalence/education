package fr.uge.esipe.db4bd.model

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

case class Venue(
    venueId: String,
    latitude: Double,
    longitude: Double,
    category: String,
    country: String
) {
  def serialize: Array[Byte] = {
    val rawId      = serializeString(venueId)
    val rawLat     = serializeDouble(latitude)
    val rawLong    = serializeDouble(longitude)
    val rawCat     = serializeString(category)
    val rawCountry = serializeString(country)

    ByteBuffer
      .allocate(
        rawId.length + rawLat.length + rawLong.length + rawCat.length + rawCountry.length
      )
      .put(rawId)
      .put(rawLat)
      .put(rawLong)
      .put(rawCat)
      .put(rawCountry)
      .rewind()
      .array()
  }
}
object Venue {
  def deserialize(buffer: ByteBuffer): Venue = {
    var length = buffer.getInt
    var content = Array.ofDim[Byte](length)
    buffer.get(content)
    val venueId = new String(content, StandardCharsets.UTF_8)

    var latitude = buffer.getDouble()
    var longitude = buffer.getDouble()

    length = buffer.getInt
    content = Array.ofDim[Byte](length)
    buffer.get(content)
    val category = new String(content, StandardCharsets.UTF_8)

    length = buffer.getInt
    content = Array.ofDim[Byte](length)
    buffer.get(content)
    val country = new String(content, StandardCharsets.UTF_8)

    Venue(venueId, latitude, longitude, category, country)
  }
}