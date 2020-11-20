package actor4fun.internal

import actor4fun.{ActorMessage, ActorRef}

case class LocalActorRef(
    override val name: String,
    system: BaseActorSystem
) extends ActorRef {

  override def sendFrom(sender: ActorRef, message: Any): Unit =
    system.actors
      .get(this)
      .map(_.pushMessage(ActorMessage(sender, message)))
      .toRight(new IllegalStateException(s"unkown actor $name"))
      .toTry
      .get

}
