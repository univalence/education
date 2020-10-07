package fr.uge.esipe.stream.net

import fr.uge.esipe.stream.net.Request.{Echo, Produce}
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import org.scalatest.funsuite.AnyFunSuiteLike

class RequestTest extends AnyFunSuiteLike {

  test("Echo serde") {
    val resquest = Echo("hello world!")
    val channel  = new MockByteChannel

    Request.write(channel, resquest)
    channel.buffer.rewind()

    val result = Request.read(channel)

    assert(result === Right(Echo("hello world!")))
  }

  test("Produce serde") {
    val resquest = Produce("user", "123AG".getBytes(), "John".getBytes())
    val channel  = new MockByteChannel

    Request.write(channel, resquest)
    channel.buffer.rewind()

    val Right(Produce(topic, key, value)) = Request.read(channel)

    assert(topic === "user")
    assert(new String(key) === "123AG")
    assert(new String(value) === "John")
  }

}

class MockByteChannel extends ByteChannel {
  val buffer: ByteBuffer = ByteBuffer.allocate(1024)

  override def read(dst: ByteBuffer): Int = {
    val start     = dst.position()
    val tmpBuffer = buffer.duplicate().slice().limit(dst.limit())
    val end       = dst.put(tmpBuffer).position()
    buffer.position(end)

    end - start
  }

  override def write(src: ByteBuffer): Int = {
    val start = buffer.position()
    val end   = buffer.put(src).position()

    end - start
  }

  override def isOpen: Boolean = true

  override def close(): Unit = {}
}
