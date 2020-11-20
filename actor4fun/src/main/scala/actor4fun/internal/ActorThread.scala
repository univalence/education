package actor4fun.internal

import actor4fun.{Actor, ActorMessage, ActorProperties, ActorRef, Shutdownable}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import org.slf4j.{Logger, LoggerFactory}
import scala.annotation.tailrec

/**
  * Provide running context to a single actor.
  *
  * This class acts as a wrapper for actors. It provides:
  *   - an inbox (blocking queue)
  *   - an async interface to push messages into the inbox
  *   - a sync process that take messages from the inbox and push
  *   them to the actor
  *   - notification that the actor is starting is shut down
  *
  * @param actor actor to wrap
  * @param properties
  */
private[internal] class ActorThread(
    name: String,
    actor: Actor,
    properties: ActorProperties
) extends Shutdownable {
  val logger: Logger = LoggerFactory.getLogger(s"$name-thread")

  val inbox: BlockingQueue[ActorMessage] = new LinkedBlockingDeque()
  val isRunning: AtomicBoolean           = new AtomicBoolean(false)

  /**
    * Add a message in the actor inbox.
    *
    * @param message message to add.
    */
  def pushMessage(message: ActorMessage): Unit = {
    logger.debug(s"""push message $message""")
    inbox.put(message)
  }

  /**
    * Inbox reading loop.
    *
    * @param self self reference of the wrapped actor.
    */
  @tailrec
  private def loop(self: ActorRef): Unit =
    if (isRunning.get()) {
      Option(inbox.poll(properties.pollTimeout._1, properties.pollTimeout._2))
        .foreach(message => processMessage(self, message))
      loop(self)
    } else ()

  private def processMessage(
      self: ActorRef,
      actorMessage: ActorMessage
  ): Unit = {
    actor.receive(actorMessage.sender)(self)(actorMessage.message)
  }

  /**
    * Signal the wrapped actor that its thread has started and launch
    * inbox reading loop.
    *
    * @param self self reference of the wrapped actor.
    */
  def start(self: ActorRef): Unit = {
    if (isRunning.compareAndSet(false, true)) {
      logger.debug(s"starting actor thread")
      actor.onStart(self)

      loop(self)
    }
  }

  /**
    * Signal the wrapped actor that its thread has stopped.
    */
  override def shutdown(): Unit = {
    if (isRunning.compareAndSet(true, false)) {
      logger.debug("stopping actor")
      actor.onShutdown()
    }
  }
}
