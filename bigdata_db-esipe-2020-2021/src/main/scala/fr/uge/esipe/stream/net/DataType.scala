package fr.uge.esipe.stream.net

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

/**
  * Represents possible types of unit data sent over the network.
  *
  * @tparam A corresponding Scala type
  */
sealed trait DataType[A] {

  /**
    * Size in bytes of the given data
    *
    * @param data data from which the size has to be computed
    * @return
    */
  def byteSizeOf(data: Any): Int

  def write(buffer: ByteBuffer, data: Any): Unit
  def read(buffer: ByteBuffer): A

  def createBuffer(data: A): ByteBuffer =
    ByteBuffer.allocate(byteSizeOf(data))
}
object DataType {

  // ---- COMPLEX TYPES

  /**
    * Defines the composition of structured values (see [[STRUCT]]).
    *
    * @param fields named fields (see [[Field]])
    */
  case class Schema(fields: Field[_]*) extends DataType[STRUCT] {
    val boundedFields: List[BoundedField[_]] =
      fields.zipWithIndex.map { case (f, i) => BoundedField(f, i, this) }.toList

    val fieldsByName: Map[String, BoundedField[_]] =
      boundedFields.groupBy(_.field.name).view.mapValues(_.head).toMap

    override def byteSizeOf(struct: Any): Int =
      fields
        .map(f =>
          struct
            .asInstanceOf[STRUCT]
            .get(f.name)
            .map(d => f.dataType.byteSizeOf(d))
        )
        .foldLeft(None: Option[Int]) {
          case (_, None)                 => None
          case (None, Some(size))        => Some(size)
          case (Some(total), Some(size)) => Some(size + total)
        }
        .getOrElse(-1)

    override def createBuffer(struct: STRUCT): ByteBuffer =
      ByteBuffer.allocate(byteSizeOf(struct))

    override def write(buffer: ByteBuffer, struct: Any): Unit = {
      boundedFields.foreach { f =>
        val value = struct.asInstanceOf[STRUCT].get(f.field.name).get
        val dt    = f.field.dataType
        dt.write(buffer, value)
      }
    }

    override def read(buffer: ByteBuffer): STRUCT = {
      val struct = STRUCT(this, Array.ofDim[Any](boundedFields.size))
      boundedFields.foreach { f =>
        struct.set(f.field.name, f.field.dataType.read(buffer))
      }

      struct
    }
  }

  /**
    * Represents a named field in structured value (see [[STRUCT]]).
    *
    * @param name name of field
    * @param dataType type of the field
    * @tparam A corresponding Scala type
    */
  case class Field[A](name: String, dataType: DataType[A])
  object Field {
    def Int8(name: String): Field[Byte]             = Field(name, INT8)
    def Int16(name: String): Field[Short]           = Field(name, INT16)
    def Int32(name: String): Field[Int]             = Field(name, INT32)
    def Int64(name: String): Field[Long]            = Field(name, INT64)
    def Str(name: String): Field[String]            = Field(name, STRING)
    def ByteArray(name: String): Field[Array[Byte]] = Field(name, BYTEARRAY)
    def Arr[A: ClassTag](name: String, dataType: DataType[A]): Field[Array[A]] =
      Field(name, new ArrayOf[A](dataType))
    def Struct(name: String, schema: Schema): Field[STRUCT] =
      Field(name, schema)
  }

  /**
   * Field bounded to a schema.
   *
   * @param field corresponding field
   * @param index position in the schema
   * @param schema schema
   * @tparam A corresponding Scala type
   */
  case class BoundedField[A](field: Field[A], index: Int, schema: Schema)

  /**
   * Represents structured values
   *
   * @param schema schema of the structure
   * @param values values to associate to the structure
   */
  class STRUCT(schema: Schema, values: Array[Any]) {
    def set[A](name: String, value: A): Unit = {
      schema.fieldsByName
        .get(name)
        .foreach(f => values(f.index) = value.asInstanceOf[Any])
    }

    def get(name: String): Option[Any] = {
      schema.fieldsByName.get(name).map(f => values(f.index))
    }

    override def toString: String = {
      val stFields =
        schema.fields
          .zip(values)
          .map { case (f, v) => s"${f.name}=$v" }
          .mkString(", ")

      s"Struct($stFields)"
    }
  }
  object STRUCT {
    def apply(schema: Schema, values: Array[Any]): STRUCT =
      new STRUCT(schema, values)
  }

  /**
   * Define collection of data of the same type.
   *
   * @param dataType type of the elements
   * @tparam A corresponding Scala type
   */
  class ArrayOf[A: ClassTag](dataType: DataType[A]) extends DataType[Array[A]] {
    def byteSizeOf(data: Any): Int = {
      INT16.byteSize + data.asInstanceOf[Array[A]].foldLeft(0) {
        case (size, d) => dataType.byteSizeOf(d) + size
      }
    }

    def write(buffer: ByteBuffer, o: Any): Unit = {
      val data = o.asInstanceOf[Array[A]]
      INT16.write(buffer, data.length.toShort)

      for (d <- data) {
        dataType.write(buffer, d)
      }
    }

    def read(buffer: ByteBuffer): Array[A] = {
      val size = INT16.read(buffer)
      val dst  = Array.ofDim[A](size)
      for (i <- 0 until size) {
        dst(i) = dataType.read(buffer)
      }

      dst
    }
  }

  // ---- PRIMITIVE TYPES

  case object INT8 extends DataType[Byte] {
    val byteSize: Int = 1

    override def byteSizeOf(data: Any): Int = byteSize

    override def write(buffer: ByteBuffer, data: Any): Unit =
      buffer.put(data.asInstanceOf[Byte])

    override def read(buffer: ByteBuffer): Byte =
      buffer.get()
  }

  case object INT16 extends DataType[Short] {
    val byteSize: Int = 2

    override def byteSizeOf(data: Any): Int = byteSize

    override def write(buffer: ByteBuffer, data: Any): Unit =
      buffer.putShort(data.asInstanceOf[Short])

    override def read(buffer: ByteBuffer): Short =
      buffer.getShort()
  }

  case object INT32 extends DataType[Int] {
    val byteSize: Int = 4

    override def byteSizeOf(data: Any): Int = byteSize

    override def write(buffer: ByteBuffer, data: Any): Unit =
      buffer.putInt(data.asInstanceOf[Int])

    override def read(buffer: ByteBuffer): Int =
      buffer.getInt()
  }

  case object INT64 extends DataType[Long] {
    val byteSize: Int = 8

    override def byteSizeOf(data: Any): Int = byteSize

    override def write(buffer: ByteBuffer, data: Any): Unit =
      buffer.putLong(data.asInstanceOf[Long])

    override def read(buffer: ByteBuffer): Long =
      buffer.getLong()
  }

  case object STRING extends DataType[String] {
    override def byteSizeOf(data: Any): Int = {
      INT16.byteSize + data
        .asInstanceOf[String]
        .getBytes(StandardCharsets.UTF_8)
        .length
    }

    override def write(buffer: ByteBuffer, data: Any): Unit = {
      val raw = data.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      buffer.putShort(raw.length.toShort)
      buffer.put(raw)
    }

    override def read(buffer: ByteBuffer): String = {
      val length = buffer.getShort()
      val result =
        if (buffer.hasArray) {
          new String(
            buffer.array(),
            buffer.position() + buffer.arrayOffset(),
            length,
            StandardCharsets.UTF_8
          )
        } else {
          val dest = Array.ofDim[Byte](length)
          buffer.get(dest)

          new String(dest, StandardCharsets.UTF_8)
        }
      buffer.position(buffer.position() + length)

      result
    }
  }

  case object BYTEARRAY extends DataType[Array[Byte]] {
    override def byteSizeOf(data: Any): Int = {
      INT16.byteSize + data
        .asInstanceOf[Array[Byte]]
        .length
    }

    override def write(buffer: ByteBuffer, data: Any): Unit = {
      val raw = data.asInstanceOf[Array[Byte]]
      buffer.putShort(raw.length.asInstanceOf[Short])
      buffer.put(raw)
    }

    override def read(buffer: ByteBuffer): Array[Byte] = {
      val length = buffer.getShort()
      val result =
        if (buffer.hasArray) {
          val dest = Array.ofDim[Byte](length)
          System.arraycopy(
            buffer.array(),
            buffer.position() + buffer.arrayOffset(),
            dest,
            0,
            length
          )

          dest
        } else {
          val dest = Array.ofDim[Byte](length)
          buffer.get(dest)

          dest
        }
      buffer.position(buffer.position() + length)

      result
    }
  }

}
