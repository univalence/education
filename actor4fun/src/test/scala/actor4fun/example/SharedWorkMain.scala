package actor4fun.example

import actor4fun.{Actor, ActorRef, ActorSystem}
import actor4fun.example.SharedWorkMessage.{GetWork, Work}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import org.slf4j.{Logger, LoggerFactory}

object SharedWorkMain {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")
    sys.addShutdownHook(system.shutdown())

    val queue = new LinkedBlockingQueue[String]()
    val leader =
      system.registerAndManage("reading-leader", new ReadingLeader(queue))
    val wleader =
      system.registerAndManage("writing-leader", new WritingLeader(queue))
    val workers =
      (1 to 5).map(id =>
        system.registerAndManage(
          s"worker-$id",
          new SharedWorkWorker(id.toString, wleader)
        )
      )

    (1 to 10).foreach { i =>
      leader.sendFrom(leader, Work(s"hello-$i"))
      Thread.sleep(500)
    }

    system.awaitTermination()
  }
}

class ReadingLeader(val queue: BlockingQueue[String]) extends Actor {
  val logger: Logger = LoggerFactory.getLogger("reading-leader")

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Work(message) =>
      logger.debug(s"push $message")
      queue.put(message)
  }
}

class WritingLeader(val queue: BlockingQueue[String]) extends Actor {
  val logger: Logger = LoggerFactory.getLogger("writing-leader")

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case GetWork(id) =>
      val message = queue.take()
      sender ! Work(message)
      logger.debug(s"$message sent to id=$id")
  }
}

class SharedWorkWorker(id: String, leader: ActorRef) extends Actor {
  import SharedWorkMessage._

  val logger: Logger = LoggerFactory.getLogger(s"worker-$id")

  override def onStart(implicit self: ActorRef): Unit = {
    Thread.sleep(500)
    leader ! GetWork(id)
  }

  override def receive(
      sender: ActorRef
  )(implicit self: ActorRef): PartialFunction[Any, Unit] = {
    case Work(message) =>
      logger.info(s"""received "$message"""")
      leader ! GetWork(id)
  }
}

object SharedWorkMessage {
  case class GetWork(id: String)
  case class Work(message: String)
}
