package actor4fun

import actor4fun.internal.{LocalActorSystem, RemoteActorSystem}
import java.util.TimerTask
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext

/**
  * Registry for actors.
  */
trait ActorSystem {

  /**
    * Register an actor with its name and manage its communication.
    *
    * @param actor actor to register
    * @param name name under which the actor should be registered
    * @return a reference to the registered actor
    */
  def registerAndManage(name: String, actor: Actor): ActorRef

  /**
    * Register an actor with its name and manage its communication.
    *
    * @param actor actor to register
    * @param name name under which the actor should be registered
    * @param actorProperties properties to set to this actor
    * @return a reference to the registered actor
    */
  def registerAndManage(
      name: String,
      actor: Actor,
      actorProperties: ActorProperties
  ): ActorRef

  /**
    * Get set of registered actor names.
    *
    * This method is specifically used to send the registered actors
    * by one actor system to a remote one.
    *
    * @return set of registered actor names.
    */
  def actorNames: Set[String]

  /**
    * Retrieve an actor reference by its name.
    *
    * @param name actor to retrieve
    * @return the actor reference or None if the name is unknown.
    */
  def findActorForName(name: String): Option[ActorRef]

  /**
    * Unregister and stop an actor.
    *
    * @param actorRef reference of the actor to unregister.
    */
  def unregisterAndStop(actorRef: ActorRef): Unit

  /**
    * Shutdown this actor system and all its registered actors.
    */
  def shutdown(): Unit

  /**
    * Wait for actor system thread to stop.
    */
  def awaitTermination(): Unit

  /**
   * Schedule repetitive tasks.
   *
   * @param delay time to wait before starting the first task.
   * @param period time to wait between the end of a task and its following starts.
   * @param task the task to schedule.
   * @return a reference the task that can be used to cancel it.
   */
  def schedule(delay: Int, period: Int)(task: () => Unit): ScheduleTask

}
object ActorSystem {
  def createLocal(name: String, properties: ActorSystemProperties)(implicit
      ec: ExecutionContext
  ): LocalActorSystem = new LocalActorSystem(name, properties)(ec)

  def createLocal(
      name: String
  )(implicit ec: ExecutionContext): LocalActorSystem =
    new LocalActorSystem(
      name,
      ActorSystemProperties(
        shutdownTimeout = (5, TimeUnit.SECONDS)
      )
    )(ec)

  def createRemote(
      name: String,
      host: String,
      port: Int,
      properties: ActorSystemProperties
  )(implicit ec: ExecutionContext): RemoteActorSystem =
    new RemoteActorSystem(name, host, port, properties)

  def createRemote(
      name: String,
      host: String,
      port: Int
  )(implicit ec: ExecutionContext): RemoteActorSystem =
    createRemote(
      name,
      host,
      port,
      ActorSystemProperties(
        shutdownTimeout = (5, TimeUnit.SECONDS)
      )
    )
}

class ScheduleTask private[actor4fun] (timerTask: TimerTask)
    extends Shutdownable {
  override def shutdown(): Unit = timerTask.cancel()
}

/**
 * Internal representation of a message sent between actors.
 *
 * @param sender sender of the message.
 * @param message content.
 */
case class ActorMessage(sender: ActorRef, message: Any)

case class ActorError(throwable: Throwable)

case class ActorSystemProperties(
    shutdownTimeout: (Long, TimeUnit)
)
case class ActorProperties(
    pollTimeout: (Long, TimeUnit)
)


