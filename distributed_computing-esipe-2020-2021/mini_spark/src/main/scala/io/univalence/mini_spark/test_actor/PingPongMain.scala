package io.univalence.mini_spark.test_actor

import actor4fun.internal.RemoteActorRef
import actor4fun.{Actor, ActorRef, ActorSystem}

object PingMain {

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createRemote("actor-system", "127.0.0.1", 10001)

    val ping = system.registerAndManage("ping", new PingPong("Ping"))
    val pong = RemoteActorRef.fromURI("actor://127.0.0.1:10002/pong", system).get

    pong.sendFrom(ping, Ball)

    system.awaitTermination()
    sys.addShutdownHook(system.shutdown())
  }

}

object PongMain {

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createRemote("actor-system", "127.0.0.1", 10002)

    val pong = system.registerAndManage("pong", new PingPong("Pong"))
    val ping =
      RemoteActorRef.fromURI("actor://127.0.0.1:10001/ping", system)

    system.awaitTermination()
    sys.addShutdownHook(system.shutdown())
  }

}

case object Ball
case object Miss

class PingPong(name: String) extends Actor {
  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Ball =>
      println(name)
      Thread.sleep(500)
      sender ! Ball
  }
}
