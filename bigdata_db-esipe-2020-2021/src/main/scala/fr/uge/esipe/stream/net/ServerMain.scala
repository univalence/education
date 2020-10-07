package fr.uge.esipe.stream.net

import fr.uge.esipe.stream.log._
import fr.uge.esipe.stream.net.Response.FetchRecord
import java.net.InetSocketAddress
import java.nio.channels.{ByteChannel, ServerSocketChannel}
import java.nio.file.{Files, Paths}
import scala.util.Using

object ServerMain {
//  val inMemoryBroker = new InMemoryBroker(3)

  val broker: Broker = {
    val workDir = Paths.get("stream")
    if (!workDir.toFile.exists()) Files.createDirectories(workDir)

    new Broker(workDir)
  }

  def main(args: Array[String]): Unit = {
    Using(ServerSocketChannel.open()) { serverSocketChannel =>
      serverSocketChannel.bind(new InetSocketAddress(19092))

      while (true) {
        Using(serverSocketChannel.accept()) { client =>
          val requestOrError: Either[RequestError, Request] =
            readRequest(client)
          println(s"got $requestOrError")

          val response: Response =
            requestOrError
              .map(processRequest)
              .left
              .map(_ => Response.ServerError("corrupted request"))
              .merge

          sendResponse(client, response)
        }.get
      }
    }.get
  }

  def processRequest(request: Request): Response =
    request match {
      case Request.Echo(message) =>
        Response.Display(message)

      case Request.Produce(topic, k, v) =>
        val record                = ProducerRecord(topic, k, v)
        val (partitionId, offset) = broker.send(record)
        Response.ProduceOk(topic, partitionId.toShort, offset)

      case Request.Fetch(topic, partitionId, offset) =>
        val records = broker.poll(topic, partitionId, offset)
        Response.FetchOk(
          topic,
          partitionId,
          records
            .map(record => FetchRecord(record.offset, record.key, record.value))
            .toList
        )

      case _ => Response.ServerError("unknown request")
    }

  def readRequest(client: ByteChannel): Either[RequestError, Request] =
    Request.read(client)

  def sendResponse(client: ByteChannel, response: Response): Unit =
    Response.write(client, response)
}

class InMemoryBroker(partitionCount: Short) {
  import scala.collection.mutable
  import scala.util.hashing.MurmurHash3

  type Raw        = Array[Byte]
  type Partitions = mutable.Seq[mutable.Queue[(Raw, Raw)]]

  val topics: mutable.Map[String, Partitions] = mutable.Map.empty

  def put(topicName: String, k: Array[Byte], v: Array[Byte]): (Short, Int) = {
    val partitionId: Short =
      (MurmurHash3.bytesHash(k).abs % partitionCount).toShort
    val topic = topics.getOrElseUpdate(
      topicName,
      mutable.Seq.fill(partitionCount)(mutable.Queue.empty)
    )
    val partition = topic(partitionId)
    partition.enqueue(k -> v)
    (partitionId, partition.size - 1)
  }

  def get(topicName: String, partitionId: Int, offset: Int): (Raw, Raw) = {
    topics(topicName)(partitionId)(offset)
  }
}
