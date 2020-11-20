package actor4fun.example

import actor4fun.{Actor, ActorRef, ActorSystem}

object HelloMain {

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val system = ActorSystem.createLocal("actor-system")
    sys.addShutdownHook(system.shutdown())

    val hello = system.registerAndManage("hello", new Hello)

    hello.sendFrom(hello, "John")

    system.awaitTermination()
  }

}

class Hello extends Actor {
  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive =
    message => println(s"Hello $message!")
}
