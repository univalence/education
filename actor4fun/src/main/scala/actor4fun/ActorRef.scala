package actor4fun

/**
  * Reference on a actor.
  *
  * The actor refence is used a unique interface to send a message to
  * actor. The reference my be a memory, a network address, a URI...
  */
trait ActorRef {

  /**
    * Name of the actor referenced by this instance.
    */
  val name: String

  /**
    * Send a message to the actor referenced by this object from a
    * given actor reference.
    *
    * The sender used to be `self`.
    *
    * @param sender reference of the actor sending the message.
    * @param message message to send
    */
  def sendFrom(sender: ActorRef, message: Any): Unit

  /**
    * Send a message to the actor referenced by this object.
    *
    * This method is used in the definition of [[Actor.receive()]] to
    * send a message to another actor, without explicitly referencing
    * `self`.
    *
    * {{{
    * override def receive(
    *     sender: ActorRef
    * )(implicit self: ActorRef): PartialFunction[Any, Unit] = {
    *   // when getting a request from an actor
    *   case Request =>
    *     // answer by sending it a response
    *     sender ! Response
    * }
    * }}}
    *
    * @param message message to send
    * @param sender reference of the actor sending the message.
    */
  def !(message: Any)(implicit sender: ActorRef): Unit =
    sendFrom(sender, message)
}


