package actor4fun

/**
  * Interface to implement by actors.
  *
  * An actor is a unitary piece of processing that reacts to messages
  * sent by other actors.
  */
trait Actor {

  type Receive = PartialFunction[Any, Unit]

  /**
    * Implement the behavior of the actor.
    *
    * @param sender sender of the message
    * @param self reference to this actor in a view to self-sending
    *             messages
    * @return message processing function
    */
  def receive(sender: ActorRef)(implicit self: ActorRef): Receive

  /**
    * Called when the actor as just started and just before it received
    * its first message.
    *
    * @param self reference to this actor in a view to self-sending
    *             messages
    */
  def onStart(implicit self: ActorRef): Unit = {}

  /**
    * Called when the actor has been asked to shutdown and just before
    * it effectively shuts down.
    */
  def onShutdown(): Unit = {}
}
