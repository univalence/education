package fr.uge.esipe.stream.net

import java.nio.ByteBuffer
import java.nio.channels.ByteChannel

import fr.uge.esipe.stream.net.DataType._

sealed trait Request {
  val opId: Short
  val schema: Schema

  def toStruct: STRUCT
  def headerStruct: STRUCT =
    STRUCT(
      Request.HEADER_SCHEMA,
      Array(Request.magic, opId, schema.byteSizeOf(toStruct))
    )
}
sealed trait RequestError
object Request {
  val magic: Short = 0xf5

  val ECHO_OPID: Short    = 0x01
  val PRODUCE_OPID: Short = 0x02
  val FETCH_OPID: Short   = 0x03

  val HEADER_SCHEMA: Schema = Schema(
    Field.Int16("magic"),
    Field.Int16("operation_id"),
    Field.Int32("message_size")
  )

  val ECHO_SCHEMA: Schema = Schema(
    Field.Str("message")
  )

  val PRODUCE_SCHEMA: Schema = Schema(
    Field.Str("topic"),
    Field.ByteArray("key"),
    Field.ByteArray("value")
  )

  val FETCH_SCHEMA: Schema = Schema(
    Field.Str("topic"),
    Field.Int16("partition_id"),
    Field.Int32("offset")
  )

  case class Echo(message: String) extends Request {
    override val opId: Short    = ECHO_OPID
    override val schema: Schema = ECHO_SCHEMA

    override def toStruct: STRUCT =
      STRUCT(schema, Array(message))
  }
  object Echo {
    def fromStruct(struct: STRUCT): Option[Echo] =
      for {
        message <- struct.get("message").map(_.asInstanceOf[String])
      } yield Echo(message)
  }

  case class Produce(topic: String, key: Array[Byte], value: Array[Byte])
      extends Request {
    override val opId: Short    = PRODUCE_OPID
    override val schema: Schema = PRODUCE_SCHEMA

    override def toStruct: STRUCT =
      STRUCT(schema, Array(topic, key, value))
  }
  object Produce {
    def fromStruct(struct: STRUCT): Option[Produce] =
      for {
        topic <- struct.get("topic").map(_.asInstanceOf[String])
        key   <- struct.get("key").map(_.asInstanceOf[Array[Byte]])
        value <- struct.get("value").map(_.asInstanceOf[Array[Byte]])
      } yield Produce(topic, key, value)
  }

  case class Fetch(topic: String, partitionId: Short, offset: Int)
      extends Request {
    override val opId: Short    = FETCH_OPID
    override val schema: Schema = FETCH_SCHEMA

    override def toStruct: STRUCT =
      STRUCT(schema, Array(topic, partitionId, offset))
  }
  object Fetch {
    def fromStruct(struct: STRUCT): Option[Fetch] =
      for {
        topic       <- struct.get("topic").map(_.asInstanceOf[String])
        partitionId <- struct.get("partition_id").map(_.asInstanceOf[Short])
        offset      <- struct.get("offset").map(_.asInstanceOf[Int])
      } yield Fetch(topic, partitionId, offset)
  }

  case object BadMagicNumber   extends RequestError
  case object UnknownOperation extends RequestError
  case object BadMessageSize   extends RequestError
  case object CorruptedData    extends RequestError

  def write(channel: ByteChannel, request: Request): Unit = {
    val headerStruct = request.headerStruct
    val headerBuffer = HEADER_SCHEMA.createBuffer(headerStruct)
    HEADER_SCHEMA.write(headerBuffer, headerStruct)
    headerBuffer.rewind()

    val struct = request.toStruct
    val buffer = request.schema.createBuffer(struct)
    request.schema.write(buffer, struct)
    buffer.rewind()

    channel.write(headerBuffer)
    channel.write(buffer)
  }

  def read(channel: ByteChannel): Either[RequestError, Request] = {
    val defaultStruct = STRUCT(HEADER_SCHEMA, Array(magic, -1, -1))
    val headerBuffer  = HEADER_SCHEMA.createBuffer(defaultStruct)
    channel.read(headerBuffer)
    headerBuffer.rewind()
    val headerStruct: STRUCT = HEADER_SCHEMA.read(headerBuffer)

    val magicPart = headerStruct.get("magic").map(_.asInstanceOf[Short])
    val operationId =
      headerStruct.get("operation_id").map(_.asInstanceOf[Short])
    val messageSize =
      headerStruct.get("message_size").getOrElse(-1).asInstanceOf[Int]

    if (magicPart.isEmpty || magicPart.get != magic) Left(BadMagicNumber)
    else if (operationId.isEmpty) Left(UnknownOperation)
    else if (messageSize < 0) Left(BadMessageSize)
    else {
      val buffer = ByteBuffer.allocate(messageSize)
      channel.read(buffer)
      buffer.rewind()

      operationId.get match {
        case ECHO_OPID =>
          val struct = ECHO_SCHEMA.read(buffer)
          Echo.fromStruct(struct).toRight(CorruptedData)

        case PRODUCE_OPID =>
          val struct = PRODUCE_SCHEMA.read(buffer)
          Produce.fromStruct(struct).toRight(CorruptedData)

        case FETCH_OPID =>
          val struct = FETCH_SCHEMA.read(buffer)
          Fetch.fromStruct(struct).toRight(CorruptedData)

        case _ =>
          Left(UnknownOperation)
      }
    }
  }
}
