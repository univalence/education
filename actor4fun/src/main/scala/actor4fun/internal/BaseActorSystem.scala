package actor4fun.internal

import actor4fun.{Actor, ActorProperties, ActorRef, ActorSystem, ActorSystemProperties, ScheduleTask, Shutdownable}
import java.util.concurrent.TimeUnit
import java.util.{Timer, TimerTask}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Common implementation for actor systems.
  *
  * @param name actor system name
  * @param properties actor system properties
  * @param ec thread pool used by the actor system
  */
private[internal] abstract class BaseActorSystem(
    val name: String,
    val properties: ActorSystemProperties,
    implicit val ec: ExecutionContext
) extends Shutdownable
    with ActorSystem {
  val actors: mutable.Map[ActorRef, ActorThread]          = mutable.Map()
  val threads: mutable.Map[ActorRef, Future[ActorThread]] = mutable.Map()
  val refs: mutable.Map[String, ActorRef]                 = mutable.Map()
  val timer: Timer                                        = new Timer(s"$name-timer")

  val logger: Logger = LoggerFactory.getLogger(name)

  /** @inheritdoc */
  override def registerAndManage(name: String, actor: Actor): ActorRef =
    registerAndManage(
      name,
      actor,
      ActorProperties(
        pollTimeout = (100, TimeUnit.MILLISECONDS)
      )
    )

  /** @inheritdoc */
  override def actorNames: Set[String] = refs.keys.toSet

  /** @inheritdoc */
  override def findActorForName(name: String): Option[ActorRef] =
    refs.get(name)

  /** @inheritdoc */
  override def unregisterAndStop(actorRef: ActorRef): Unit = {
    logger.debug(s"unregistering $name")

    val actor = actors(actorRef)
    actor.shutdown()

    actors.remove(actorRef)
    threads.remove(actorRef)
    refs.remove(actorRef.name)
  }

  /** @inheritdoc */
  override def schedule(delay: Int, period: Int)(
      task: () => Unit
  ): ScheduleTask = {
    val timerTask: TimerTask = new TimerTask {
      override def run(): Unit = task()
    }

    timer.schedule(timerTask, delay, period)

    new ScheduleTask(timerTask)
  }

}
