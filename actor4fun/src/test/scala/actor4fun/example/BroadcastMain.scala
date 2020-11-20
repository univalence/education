package actor4fun.example

import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import BroadcastMessage._
import actor4fun.{Actor, ActorRef, ActorSystem}

object BroadcastMain {

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")
    sys.addShutdownHook(system.shutdown())

    val leader = system.registerAndManage("leader", new BroadcastLeader())
    val workers =
      (1 to 5).map(id =>
        system.registerAndManage(
          s"worker-$id",
          new BroadcastWorker(id.toString, leader)
        )
      )

    (1 to 10).foreach { i =>
      leader.sendFrom(leader, Broadcast(s"hello-$i"))
      Thread.sleep(500)
    }

    system.awaitTermination()
  }
}

class BroadcastLeader() extends Actor {
  val logger: Logger = LoggerFactory.getLogger("leader")

  val workers: mutable.Map[String, ActorRef] = mutable.Map[String, ActorRef]()

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case AddWorker(id) =>
      workers += (id -> sender)
    case Broadcast(message) =>
      workers.values.foreach(_ ! Broadcast(message))
  }
}

class BroadcastWorker(id: String, leader: ActorRef) extends Actor {
  import BroadcastMessage._

  val logger: Logger = LoggerFactory.getLogger(s"worker-$id")

  override def onStart(implicit self: ActorRef): Unit = {
    leader ! AddWorker(id)
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Broadcast(message) =>
      logger.info(s"""received "$message"""")
  }
}

object BroadcastMessage {
  case class AddWorker(id: String)
  case class Broadcast(message: String)
}
