package fr.uge.esipe.stream.net

import fr.uge.esipe.stream.log.Broker
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.nio.file.Paths
import scala.util.Using

object ClientMain {
  def main(args: Array[String]): Unit = {
    Using(SocketChannel.open()) { socketChannel =>
      socketChannel.connect(new InetSocketAddress("127.0.0.1", 19092))

      val request =
//       Request.Produce("user", "123AG".getBytes(), "Bob".getBytes())
//       Request.Produce("user", "123AG".getBytes(), "John".getBytes())
       Request.Fetch("user", 0, 0)
//       Request.Fetch("my-topic", 0, 0)
//        Request.Echo("hello world!")
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

object CreateTopicMain {
  def main(args: Array[String]): Unit = {
    val broker = new Broker(Paths.get("stream"))
    broker.createTopic("user", 3)
  }
}
