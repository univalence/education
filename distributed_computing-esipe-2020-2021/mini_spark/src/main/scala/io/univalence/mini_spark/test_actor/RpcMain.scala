package io.univalence.mini_spark.test_actor

import actor4fun.{Actor, ActorRef, ActorSystem}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object RpcMain {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")

    val proxy = new Proxy
    val proxyActor = system.registerAndManage("proxy", proxy)
    val server = system.registerAndManage("server", new Server(proxyActor))

    Thread.sleep(100)

    val stub = new ServerStub(proxy, proxyActor)

    println(stub.greet("John"))
    println(stub.greet("Mary"))
    println(stub.greet("Alan"))
  }
}

class ServerStub(proxy: Proxy, proxyActor: ActorRef) {

  def greet(name: String): String = {
    val (id, f) = proxy.newSession
    val request = Request(id, name)

    proxyActor.sendFrom(null, request)

    val result: Response =
      Await.result(f, Duration.Inf)

    proxy.removeSession(id)

    result.value
  }

}

case class Request(id: Int, parameter: String)
case class Response(id: Int, value: String)
case object RegisterServer

class Proxy extends Actor {
  var server: ActorRef = _
  val currentId: AtomicInteger = new AtomicInteger(0)
  val sessions: mutable.Map[Int, Promise[Response]] =
    mutable.Map.empty

  def newSession: (Int, Future[Response]) = {
    val id = currentId.getAndIncrement()
    val promise = Promise[Response]

    println(s"${getClass.getSimpleName}: register session #$id")
    sessions.addOne(id -> promise)

    (id, promise.future)
  }

  def removeSession(id: Int): Unit = {
    sessions.remove(id)
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case RegisterServer =>
      server = sender

    case Request(id, parameter) =>
      println(s"${getClass.getSimpleName}: send request to server ($id, $parameter)")
      server ! Request(id, parameter)

    case r @ Response(id, value) =>
      println(s"${getClass.getSimpleName}: response receive from server ($id, $value)")
      val promise: Promise[Response] = sessions(id)
      promise.success(r)
  }
}

class Server(proxy: ActorRef) extends Actor {
  override def onStart(implicit self: ActorRef): Unit = {
    proxy ! RegisterServer
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Request(id, parameter) =>
      val result = s"Hello $parameter"
      println(s"${getClass.getSimpleName}: get request from proxy ($id, $parameter)")
      sender ! Response(id, result)
  }
}
