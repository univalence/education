package fr.uge.esipe.stream.net

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import scala.util.Using

object ClientMain {
  def main(args: Array[String]): Unit = {
    Using(SocketChannel.open()) { socketChannel =>
      socketChannel.connect(new InetSocketAddress("127.0.0.1", 19092))

      val request =
      // Request.Push("user", "123AG".getBytes(), "Bob".getBytes())
      // Request.Push("user", "123AG".getBytes(), "John".getBytes())
      // Request.Fetch("user", 0, 0)
        Request.Echo("hello world!")
      Request.write(socketChannel, request)

      val response = Response.read(socketChannel)

      response match {
        case Response.FetchOk(_, _, records) =>
          records.foreach { r =>
            val key   = new String(r.key)
            val value = new String(r.value)
            println(s"$key -> $value")
          }
        case _ =>
          println(response)
      }

    }
  }
}
