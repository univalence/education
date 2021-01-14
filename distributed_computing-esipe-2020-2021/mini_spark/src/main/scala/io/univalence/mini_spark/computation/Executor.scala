package io.univalence.mini_spark.computation

import actor4fun.{Actor, ActorRef, ActorSystem}
import io.univalence.mini_spark.computation.message._
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Using
import scala.util.control.NonFatal

class Executor(id: String, driver: ActorRef, system: ActorSystem)
    extends Actor {

  val HeartbeatInterval: Int = 5 * 1000
  val HeartbeatMaxRetry: Int = 3

  val name = s"executor-$id"

  val logger: Logger = LoggerFactory.getLogger(name)

  /**
   * Count the number of retry when the driver is not available.
   */
  var heartbeatRetry: Int = 0

  /**
   * Indicate if the heart beat task is running.
   *
   * This avoid to run the scheduled heart beat task twice or more.
   */
  var heartbeatScheduled: Boolean = false

  /**
   * Task to schedule in a view to send heart beat to the driver. If
   * the driver is not available, we get a network error and then
   * retry to a communication with the driver. If the number of retry
   * reach a limit, the executor stops.
   */
  def heartbeatSender(implicit self: ActorRef): Unit =
    try {
      driver ! Heartbeat(id)
      // if the heart beat send succeed, the retry counter is set to
      // zero
      heartbeatRetry = 0
    } catch {
      case NonFatal(e) =>
        heartbeatRetry += 1
        if (heartbeatRetry < HeartbeatMaxRetry) {
          // if the max number of retry is not reached, we just
          // increment the retry counter
          logger.warn(
            s"""driver ${driver.name} is not responding due to "${e.getMessage}". Then retry #$heartbeatRetry."""
          )
        } else {
          // if the max number of retry has been reached, we ask the
          // system to stop
          logger.warn(
            s"""driver ${driver.name} is not responding due to "${e.getMessage}". Then shutdown executor."""
          )

          self ! PoisonPill
        }
    }

  override def onStart(implicit self: ActorRef): Unit = {
    logger.info(s"registering to driver ${driver.name}")
    driver ! ExecutorRegister(id)
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    // when driver did not succeed to register this executor
    // the executor then shuts down
    case ExecutorRegisterReply(Left(errorMessage)) =>
      logger.error(s"driver get error while registering: $errorMessage")
      self ! PoisonPill

    // when driver succeeded to register this executor
    // the executor then schedule the heart beat task
    case ExecutorRegisterReply(Right(id)) =>
      logger.info(s"executor registered has id=$id")
      if (!heartbeatScheduled) {
        system.schedule(100, HeartbeatInterval) { () => heartbeatSender }
        heartbeatScheduled = true
      }

    case ExecutorRegisterAgain(_) =>
      logger.warn(s"driver ${driver.name} asked to register again")
      driver ! ExecutorRegister(id)

    // when the driver asks this executor to run a task
    case ExecutorTask(task) =>
      logger.info(s"received ${task.id} from $sender")
      // run the task
      val result: Array[Any] =
        Using(task.rdd.compute(task.partition)) {
          _.toArray
        }.get
      logger.info(s"send results of ${task.id} to $sender")
      // send back the results to the driver
      sender ! ExecutorTaskResult(task.id, RawTaskResult(task.id, task, result))

    case PoisonPill =>
      system.shutdown()
  }

  override def onShutdown(): Unit =
    logger.info(s"executor stopped (id=$id)")

}
