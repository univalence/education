package fr.uge.esipe.stream.net

import java.nio.ByteBuffer
import org.scalatest.funsuite.AnyFunSuiteLike

class DataTypeTest extends AnyFunSuiteLike {

  import DataType._

  test("int 8 serde") {
    val dt     = INT8
    val buffer = dt.createBuffer(0)
    dt.write(buffer, 42.toByte)
    buffer.rewind()
    val result = dt.read(buffer)

    assert(result === 42)
  }

  test("int 16 serde") {
    val dt     = INT16
    val buffer = dt.createBuffer(0)
    dt.write(buffer, 342.toShort)
    buffer.rewind()
    val result = dt.read(buffer)

    assert(result === 342)
  }

  test("int 32 serde") {
    val dt     = INT32
    val buffer = dt.createBuffer(0)
    dt.write(buffer, 65342.toInt)
    buffer.rewind()
    val result = dt.read(buffer)

    assert(result === 65342)
  }

  test("int 64 serde") {
    val dt     = INT64
    val buffer = dt.createBuffer(0)
    dt.write(buffer, 65342L)
    buffer.rewind()
    val result = dt.read(buffer)

    assert(result === 65342L)
  }

  test("string serde") {
    val buffer = STRING.createBuffer("hello world!")
    STRING.write(buffer, "hello world!")
    buffer.rewind()
    val result = STRING.read(buffer)

    assert(result === "hello world!")
  }

  test("byte array serde") {
    val buffer = BYTEARRAY.createBuffer("hello world!".getBytes())
    BYTEARRAY.write(buffer, "hello world!".getBytes())
    buffer.rewind()
    val result = BYTEARRAY.read(buffer)

    assert(result === "hello world!".getBytes())
  }

  test("array of int8 serde") {
    val arrayInt8 = new ArrayOf(INT8)
    val data      = Array[Byte](1, 2, 3, 4)
    val buffer    = ByteBuffer.allocate(arrayInt8.byteSizeOf(data))
    arrayInt8.write(buffer, data)
    buffer.rewind()
    val result = arrayInt8.read(buffer).toList

    assert(result === List(1, 2, 3, 4))
  }

  test("array of string serde") {
    val arrayString = new ArrayOf(STRING)
    val data        = Array[String]("key1", "key2")
    val buffer      = ByteBuffer.allocate(arrayString.byteSizeOf(data))
    arrayString.write(buffer, data)
    buffer.rewind()
    val result = arrayString.read(buffer).toList

    assert(result === List("key1", "key2"))
  }

  test("one field struct set/get") {
    val schema = Schema(Field.Int16("id"))
    val struct = STRUCT(schema, Array.ofDim(1))

    struct.set("id", 42.toShort)

    assert(struct.get("id") === Some(42))
  }

  test("one field struct serde") {
    val schema = Schema(Field.Int16("id"))
    val struct = STRUCT(schema, Array(42.toShort))

    val buffer = schema.createBuffer(struct)
    schema.write(buffer, struct)
    buffer.rewind
    val result = schema.read(buffer)

    assert(result.get("id") === Some(42))
  }

  test("two fields struct serde") {
    val schema = Schema(Field.Int16("id"), Field.Str("name"))
    val struct = STRUCT(schema, Array(42.toShort, "John"))

    val buffer = schema.createBuffer(struct)
    schema.write(buffer, struct)
    buffer.rewind
    val result = schema.read(buffer)

    assert(result.get("id") === Some(42))
    assert(result.get("name") === Some("John"))
  }

  test("two level schema") {
    val subSchema = Schema(
      Field.Int16("field2")
    )
    val schema = Schema(
      new Field("field1", subSchema)
    )
    val struct = STRUCT(schema, Array(STRUCT(subSchema, Array(42.toShort))))

    val buffer = schema.createBuffer(struct)
    schema.write(buffer, struct)
    buffer.rewind
    val result    = schema.read(buffer)
    val subStruct = result.get("field1").get.asInstanceOf[STRUCT]

    assert(subStruct.get("field2").get === 42)
  }

  test("two level with array schema") {
    val subSchema: Schema = Schema(
      Field.Int16("field2")
    )
    val arraySubSchema: ArrayOf[STRUCT] = new ArrayOf(subSchema)
    val schema = Schema(
      new Field("field1", arraySubSchema)
    )
    val struct =
      STRUCT(schema, Array(Array(STRUCT(subSchema, Array(42.toShort)))))

    val buffer = schema.createBuffer(struct)
    schema.write(buffer, struct)
    buffer.rewind
    val result    = schema.read(buffer)
    val subStruct = result.get("field1").get.asInstanceOf[Array[STRUCT]]

    assert(subStruct(0).get("field2").get === 42)
  }

}
