package actor4fun.example

import actor4fun.example.HeartbeatMessage._
import actor4fun.{Actor, ActorRef, ActorSystem, ScheduleTask}
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.util.Random

object HeartbeatMain {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._
    val system = ActorSystem.createLocal("actor-system")
    sys.addShutdownHook(system.shutdown())

    val receiver =
      system.registerAndManage("receiver", new HeartbeatReceiver(system))
    val emitter1 = system.registerAndManage(
      s"emitter-1",
      new HeartbeatEmitter("1", receiver, system)
    )
    val emitter2 = system.registerAndManage(
      s"emitter-2",
      new HeartbeatEmitter("2", receiver, system)
    )

    Thread.sleep(5000)
    system.unregisterAndStop(receiver)
//    Thread.sleep(10000)
//    system.registerAndManage(s"receiver", new HeartbeatReceiver(system))

    system.awaitTermination()
  }
}

class HeartbeatReceiver(system: ActorSystem) extends Actor {
  val logger: Logger                                = LoggerFactory.getLogger(s"Receiver")
  val emitters: mutable.Map[String, ActorRef]       = mutable.Map()
  val emitterLastBeat: mutable.Map[String, Instant] = mutable.Map()

  def emitterReaper(): Unit = {
    val beatLimit = Instant.now().minus(10, ChronoUnit.SECONDS)

    emitterLastBeat
      .filter { case (_, lastBeat) => lastBeat.isBefore(beatLimit) }
      .keys
      .foreach { id =>
        logger.info(s"remove emitter-$id (heart beat too old)")
        emitters.remove(id)
        emitterLastBeat.remove(id)
      }
  }

  override def onStart(implicit self: ActorRef): Unit = {
    system.schedule(100, 1000)(emitterReaper)
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case Register(id) =>
      if (emitters.contains(id)) {
        logger.warn(s"emitter-$id already registered")
        sender ! RegisterKO(id, AlreadyRegistered)
      } else {
        emitters += (id        -> sender)
        emitterLastBeat += (id -> Instant.now())
        logger.info(s"emitter-$id is registered")
        sender ! RegisterOK(id)
      }

    case HeartbeatSignal(id) =>
      if (!emitters.contains(id)) {
        logger.warn(s"emitter-$id unknown. Ask to register.")
        sender ! RegisterAgain(id)
      } else {
        logger.info(s"emitter-$id sent heartbeat")
        emitterLastBeat += (id -> Instant.now())
      }
  }
}

class HeartbeatEmitter(id: String, var receiver: ActorRef, system: ActorSystem)
    extends Actor {
  val MaxHeartbeatRetry: Int = 5

  val logger: Logger                      = LoggerFactory.getLogger(s"Emitter-$id")
  var heartbeatScheduleTask: ScheduleTask = _
  var heartbeatRetry: Int                 = 0

  override def onStart(implicit self: ActorRef): Unit = {
    logger.info(s"registering...")
    receiver ! Register(id)
  }

  override def receive(sender: ActorRef)(implicit self: ActorRef): Receive = {
    case RegisterOK(_) =>
      logger.info(s"register OK. Schedule send of heartbeat")
      startHeartbeat

    case RegisterKO(_, AlreadyRegistered) =>
      logger.warn(s"already registered")
      if (heartbeatScheduleTask == null)
        startHeartbeat

    case RegisterKO(_, reason) =>
      logger.warn(s"registering failed due to $reason")
      logger.warn(s"shutting down")
      system.unregisterAndStop(self)

    case RegisterAgain(_) =>
      logger.warn(s"asked to register again")
      if (heartbeatScheduleTask != null) heartbeatScheduleTask.shutdown()
      receiver ! Register(id)
  }

  private def startHeartbeat(implicit self: ActorRef): Unit = {
    heartbeatScheduleTask =
      system.schedule(100 + Random.nextInt(1000), 5000)(() => heartbeatTask)
  }

  def heartbeatTask(implicit self: ActorRef): Unit = {
    logger.info(s"send heartbeat")
    try {
      receiver ! HeartbeatSignal(id)
      heartbeatRetry = 0
    } catch {
      case e: IllegalStateException =>
        if (heartbeatRetry < MaxHeartbeatRetry) {
          logger.warn(
            s"cannot send heartbeat to receiver. Retry #$heartbeatRetry"
          )
          heartbeatRetry += 1
        } else {
          logger.warn(s"cannot send heartbeat to receiver. Then shutting down.")
          system.unregisterAndStop(self)
        }
    }
  }

  override def onShutdown(): Unit = {
    if (heartbeatScheduleTask != null) heartbeatScheduleTask.shutdown()
    logger.info("shut dowm")
  }
}

object HeartbeatMessage {
  sealed trait Error
  case object AlreadyRegistered extends Error

  case class Register(id: String)
  case class RegisterOK(id: String)
  case class RegisterKO(id: String, reason: Error)

  case class HeartbeatSignal(id: String)
  case class RegisterAgain(id: String)
}
