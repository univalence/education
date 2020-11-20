package actor4fun.example

import actor4fun.example.SyncCommMessage.{Echo, EchoRequest, EchoResponse}
import actor4fun.{Actor, ActorRef, ActorSystem}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object SyncCommMain {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")

    val server1 = system.registerAndManage("server-1", new EchoServer("hello"))
    val server2 = system.registerAndManage("server-2", new EchoServer("bye"))

    val proxy = new Stub(List(server1, server2), system)

    val tasks: Seq[String] =
      Seq(
        "John",
        "Mary",
        "Marc",
        "Daniel"
      )

    val futures: Seq[Future[Unit]] =
      tasks
        .map(t =>
          Future {
            proxy.echo(t).foreach(println)
          }
        )

    futures.foreach(t => Await.ready(t, Duration.Inf))

    system.shutdown()
  }
}

class Stub(servers: Seq[ActorRef], system: ActorSystem) {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  val proxyActor: ProxyActor  = new ProxyActor(servers)
  val proxyActorRef: ActorRef = system.registerAndManage("proxy", proxyActor)

  def echo(message: String): Seq[String] = {
    logger.info(s"echo for message: $message")
    val futures: Map[String, Future[String]] = proxyActor.registerRequest()

    proxyActorRef.sendFrom(null, Echo(futures.keys.toSeq, message))

    val results: Seq[String] =
      futures.map(f => Await.result(f._2, Duration.Inf)).toSeq
    futures.foreach(f => proxyActor.unregisterRequest(f._1))

    results
  }
}

class ProxyActor(servers: Seq[ActorRef]) extends Actor {
  val logger: Logger                                   = LoggerFactory.getLogger(getClass)
  val promises: ConcurrentMap[String, Promise[String]] = new ConcurrentHashMap()
  val sequence: AtomicInteger                          = new AtomicInteger()

  def registerRequest(): Map[String, Future[String]] = {
    val sessions: Seq[String] =
      servers.map(_ => sequence.incrementAndGet().toString)
    logger.info(s"register new request with ids: ${sessions.mkString(", ")}")

    val ps = sessions.map(_ -> Promise[String]).toMap
    ps.foreach { case (id, p) => promises.put(id, p) }

    ps.map { case (id, p) => id -> p.future }
  }

  def unregisterRequest(id: String): Unit =
    promises.remove(id)

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case EchoResponse(id, message) =>
      promises.get(id).success(message)

    case Echo(ids, message) =>
      logger.info(s"get message and send it: $message")
      servers
        .zip(ids)
        .foreach {
          case (server, id) =>
            server ! EchoRequest(id, message)
        }
  }
}

class EchoServer(prefix: String) extends Actor {
  val logger: Logger = LoggerFactory.getLogger(s"server-prefix:$prefix")

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case EchoRequest(id, message) =>
      logger.info(s"request #$id: $message")
      sender ! EchoResponse(id, s"${self.name} $id: $prefix $message")
  }
}

object SyncCommMessage {
  case class Echo(ids: Seq[String], message: String)
  case class EchoRequest(id: String, message: String)
  case class EchoResponse(id: String, message: String)
}
