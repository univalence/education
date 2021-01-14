package io.univalence.mini_spark.test_actor

import actor4fun.{Actor, ActorRef, ActorSystem}
import scala.collection.mutable.ArrayBuffer

object BroadcastMain {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")

    val leader = system.registerAndManage("leader", new LeaderActor)
    val workers =
      for (i <- 1 to 5)
        yield system.registerAndManage(s"worker-$i", new Worker(leader))

    Thread.sleep(500)

    leader.sendFrom(null, Broadcast("John"))
    leader.sendFrom(null, Broadcast("Mary"))

    system.awaitTermination()
    sys.addShutdownHook(system.shutdown())
  }
}

case object AddWorker

case class Broadcast(message: String)
case class Work(message: String)

class LeaderActor extends Actor {
  val workers: ArrayBuffer[ActorRef] = ArrayBuffer.empty

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case AddWorker =>
      workers += sender
      println(s"add worker: $sender")

    case Broadcast(message) =>
      for (worker <- workers) {
        worker ! Work(message)
      }
  }
}

class Worker(leader: ActorRef) extends Actor {
  override def onStart(implicit self: ActorRef): Unit = {
    leader ! AddWorker
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Work(message) =>
      println(s"$sender: $message")
  }
}
