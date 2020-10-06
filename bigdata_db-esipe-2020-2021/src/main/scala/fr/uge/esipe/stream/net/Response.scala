package fr.uge.esipe.stream.net

import java.nio.channels.ByteChannel
import fr.uge.esipe.stream.net.DataType._
import java.nio.ByteBuffer

trait Response {
  val repId: Short
  val schema: Schema

  def toStruct: STRUCT
  def headerStruct: STRUCT =
    STRUCT(
      Response.HEADER_SCHEMA,
      Array(Response.magic, repId, schema.byteSizeOf(toStruct))
    )

}
object Response {
  val magic: Short = 0x5f

  val HEADER_SCHEMA: Schema = Schema(
    Field.Int16("magic"),
    Field.Int16("response_type"),
    Field.Int32("message_size")
  )

  val DISPLAY_SCHEMA: Schema = Schema(
    Field.Str("message")
  )

  val PUSHOK_SCHEMA: Schema = Schema(
    Field.Str("topic"),
    Field.Int16("partition_id"),
    Field.Int32("offset")
  )

  val FETCHRECORD_SCHEMA: Schema = Schema(
    Field.Int32("offset"),
    Field.ByteArray("key"),
    Field.ByteArray("value")
  )
  val FETCHOK_SCHEMA: Schema = Schema(
    Field.Str("topic"),
    Field.Int16("partition_id"),
    Field[Array[STRUCT]]("records", new ArrayOf(FETCHRECORD_SCHEMA))
  )

  val SERVERERROR_SCHEMA: Schema = Schema(
    Field.Str("message")
  )

  val NO_SCHEMA: Schema = Schema()

  case class Display(message: String) extends Response {
    override val repId: Short   = 0x01
    override val schema: Schema = DISPLAY_SCHEMA

    override def toStruct: STRUCT =
      STRUCT(schema, Array(message))
  }
  object Display {
    def fromStruct(struct: STRUCT): Option[Display] =
      for {
        message <- struct.get("message").map(_.asInstanceOf[String])
      } yield Display(message)
  }

  case class PushOk(topic: String, partitionId: Short, offset: Int)
    extends Response {
    override val repId: Short   = 0x02
    override val schema: Schema = PUSHOK_SCHEMA

    override def toStruct: STRUCT =
      STRUCT(schema, Array(topic, partitionId, offset))
  }
  object PushOk {
    def fromStruct(struct: STRUCT): Option[PushOk] =
      for {
        topic       <- struct.get("topic").map(_.asInstanceOf[String])
        partitionId <- struct.get("partition_id").map(_.asInstanceOf[Short])
        offset      <- struct.get("offset").map(_.asInstanceOf[Int])
      } yield PushOk(topic, partitionId, offset)

  }

  case class FetchRecord(offset: Int, key: Array[Byte], value: Array[Byte]) {
    val schema: Schema = FETCHRECORD_SCHEMA
    def toStruct: STRUCT =
      STRUCT(schema, Array(offset, key, value))
  }
  object FetchRecord {
    def fromStruct(struct: STRUCT): Option[FetchRecord] =
      for {
        offset <- struct.get("offset").map(_.asInstanceOf[Int])
        key    <- struct.get("key").map(_.asInstanceOf[Array[Byte]])
        value  <- struct.get("value").map(_.asInstanceOf[Array[Byte]])
      } yield FetchRecord(offset, key, value)
  }

  case class FetchOk(
                      topic: String,
                      partitionId: Short,
                      records: List[FetchRecord]
                    ) extends Response {
    override val repId: Short   = 0x03
    override val schema: Schema = FETCHOK_SCHEMA

    override def toStruct: STRUCT = {
      val arraySubStruct: Array[STRUCT] = records.map(_.toStruct).toArray
      STRUCT(schema, Array(topic, partitionId, arraySubStruct))
    }
  }
  object FetchOk {
    def fromStruct(struct: STRUCT): Option[FetchOk] =
      for {
        topic       <- struct.get("topic").map(_.asInstanceOf[String])
        partitionId <- struct.get("partition_id").map(_.asInstanceOf[Short])
        records     <- struct.get("records").map(_.asInstanceOf[Array[STRUCT]])
      } yield FetchOk(
        topic,
        partitionId,
        records.toList.map(s => FetchRecord.fromStruct(s).get)
      )

  }

  case class ServerError(message: String) extends Response {
    override val repId: Short   = 0xfa
    override val schema: Schema = SERVERERROR_SCHEMA

    override def toStruct: STRUCT =
      STRUCT(schema, Array(message))
  }

  case object BadMagicNumber extends Response {
    override val repId: Short   = 0xff
    override val schema: Schema = NO_SCHEMA

    override def toStruct: STRUCT = STRUCT(schema, Array.empty)
  }

  case object UnknownResponse extends Response {
    override val repId: Short   = 0xfe
    override val schema: Schema = NO_SCHEMA

    override def toStruct: STRUCT = STRUCT(schema, Array.empty)
  }

  case object BadMessageSize extends Response {
    override val repId: Short   = 0xfd
    override val schema: Schema = NO_SCHEMA

    override def toStruct: STRUCT = STRUCT(schema, Array.empty)
  }

  case object CorruptedData extends Response {
    override val repId: Short   = 0xfc
    override val schema: Schema = NO_SCHEMA

    override def toStruct: STRUCT = STRUCT(schema, Array.empty)
  }

  def write(channel: ByteChannel, response: Response): Unit = {
    val headerStruct = response.headerStruct
    val headerBuffer = HEADER_SCHEMA.createBuffer(headerStruct)
    HEADER_SCHEMA.write(headerBuffer, headerStruct)
    headerBuffer.rewind()

    val struct = response.toStruct
    val buffer = response.schema.createBuffer(struct)
    response.schema.write(buffer, struct)
    buffer.rewind()

    channel.write(headerBuffer)
    channel.write(buffer)
  }

  def read(channel: ByteChannel): Response = {
    val defaultStruct = STRUCT(HEADER_SCHEMA, Array(magic, -1, -1))
    val headerBuffer  = HEADER_SCHEMA.createBuffer(defaultStruct)
    channel.read(headerBuffer)
    headerBuffer.rewind()
    val headerStruct = HEADER_SCHEMA.read(headerBuffer)

    val magicPart = headerStruct.get("magic").map(_.asInstanceOf[Short])
    val repId =
      headerStruct.get("response_type").map(_.asInstanceOf[Short])
    val messageSize =
      headerStruct.get("message_size").getOrElse(-1).asInstanceOf[Int]

    if (magicPart.isEmpty || magicPart.get != magic) BadMagicNumber
    else if (repId.isEmpty) UnknownResponse
    else if (messageSize < 0) BadMessageSize
    else {
      val buffer = ByteBuffer.allocate(messageSize)
      channel.read(buffer)
      buffer.rewind()

      repId.get match {
        case 0x01 =>
          val struct = DISPLAY_SCHEMA.read(buffer)
          Display.fromStruct(struct).getOrElse(CorruptedData)
        case 0x02 =>
          val struct = PUSHOK_SCHEMA.read(buffer)
          PushOk.fromStruct(struct).getOrElse(CorruptedData)
        case 0x03 =>
          val struct = FETCHOK_SCHEMA.read(buffer)
          FetchOk.fromStruct(struct).getOrElse(CorruptedData)
        case _ =>
          UnknownResponse
      }
    }
  }

}

