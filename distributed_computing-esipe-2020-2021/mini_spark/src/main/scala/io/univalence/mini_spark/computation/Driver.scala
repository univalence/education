package io.univalence.mini_spark.computation

import actor4fun.{Actor, ActorRef, ActorSystem}
import io.univalence.mini_spark.computation.message._
import java.time.{Instant, Duration => JDuration}
import java.util.concurrent.CountDownLatch
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.concurrent.Promise

class Driver(name: String, system: ActorSystem, executorCount: Int)
    extends Actor {
  val HeartbeatTimeout: Int = 10 * 1000

  val logger: Logger = LoggerFactory.getLogger(name)

  /**
    * Registered executors
    */
  val executors: mutable.Map[String, ActorRef] = mutable.Map.empty

  /**
    * Last executor health check timestamp
    */
  val executorCheckpoints: mutable.Map[String, Instant] = mutable.Map.empty

  // hint: help to wait for a certain number of executors
  val executorCountdownLatch = new CountDownLatch(executorCount)

  /**
   * Structure that contains the currently running tasks by task ID
   */
  var taskSyncs: mutable.Map[String, Promise[Any]] = mutable.Map()

  /**
   * Register a new task to this driver. This method is used by the
   * job context to send tasks.
   */
  def registerTask[A](task: TaskContext[A]): Promise[RawTaskResult[A]] = {
    val promise = Promise[RawTaskResult[A]]
    taskSyncs.update(task.id, promise.asInstanceOf[Promise[Any]])

    promise
  }

  def removeTask(id: String): Unit = {
    taskSyncs.remove(id)
  }

  override def onStart(implicit self: ActorRef): Unit = {
    logger.info(s"driver started")
    system.schedule(100, 1000) { () => removeNotRespondingExecutors() }
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Heartbeat(executorId) =>
      if (executors.contains(executorId)) {
        // update the timestamp of the last received heart beat for
        // the given executor
        executorCheckpoints.update(executorId, Instant.now())
//        println(s"$name: executor heartbeat id=$id updated")
      } else {
        // if the executor is not registered
        logger.warn(s"executor id=$executorId unknown on heartbeat. Ask to register again.")
        sender ! ExecutorRegisterAgain(executorId)
      }

    case ExecutorRegister(id) =>
      logger.debug(s"registering executor id=$id...")
      val reply = registerExecutor(sender, id)
      executorCountdownLatch.countDown()
      sender ! ExecutorRegisterReply(reply)

    case ExecutorTaskResult(taskId, results) =>
      logger.info(s"getting results of task $taskId from $sender")
      // notifies awaiting threads that the receive task has finished
      taskSyncs(taskId).success(results)
  }

  // note: Either type here helps to manage the registration errors
  private def registerExecutor(
      executor: ActorRef,
      id: String
  ): Either[String, String] =
    if (executors.contains(id)) {
      logger.warn(s"executor id=$id is already registered")

      Left(s"executor already registered: id=${id}")
    } else {
      executors += (id -> executor)
      logger.info(s"add executor $id -> ${executor.name}")

      Right(id)
    }

  private def removeNotRespondingExecutors(): Unit = {
    val oldHeartbeats: Iterable[String] =
      executorCheckpoints
        .filter { case (_, t) => isTooOld(t, Instant.now()) }
        .keys

    logger.trace(s"executors with too old heartbeat: $oldHeartbeats")

    oldHeartbeats
      .foreach { id =>
        executors.remove(id)
        executorCheckpoints.remove(id)
        logger.info(s"executor removed id=$id")
      }
  }

  private def isTooOld(t: Instant, now: Instant) =
    JDuration
      .between(now, t)
      .abs()
      .toMillis > HeartbeatTimeout
}
