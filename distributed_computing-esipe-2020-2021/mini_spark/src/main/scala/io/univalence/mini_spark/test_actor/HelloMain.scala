package io.univalence.mini_spark.test_actor

import actor4fun.{Actor, ActorRef, ActorSystem}

object HelloMain {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")

    val helloActor: ActorRef = system.registerAndManage("hello", new HelloActor)

    helloActor.sendFrom(null, "John")

    system.awaitTermination()
    sys.addShutdownHook(system.shutdown())
  }
}

class HelloActor extends Actor {
  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case message => println(s"Hello $message")
  }
}
