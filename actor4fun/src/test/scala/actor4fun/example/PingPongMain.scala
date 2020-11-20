package actor4fun.example

import PingPongMessage._
import actor4fun.{Actor, ActorRef, ActorSystem}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Random

object PingPongMain {

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")
    sys.addShutdownHook(system.shutdown())

    val ping =
      system.registerAndManage("ping", new PingPongActor("ping", system))
    val pong =
      system.registerAndManage("pong", new PingPongActor("pong", system))

    val players = Random.shuffle(List(ping, pong))
    println(s"Service: ${players(0).name}")
    players(1).sendFrom(players(0), Ball)

    system.awaitTermination()
  }

}

class PingPongActor(name: String, system: ActorSystem) extends Actor {
  val logger: Logger = LoggerFactory.getLogger(s"$name-actor")

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Ball =>
      Thread.sleep(500)
      if (Random.nextInt(100) < 10) {
        logger.warn("missed!!!")
        sender ! Miss
        self ! Miss
      } else {
        logger.info(name.toUpperCase())
        sender ! Ball
      }

    case Miss =>
      system.shutdown()
  }
}

object PingPongMessage {
  case object Ball
  case object Miss
}
