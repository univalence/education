package actor4fun.internal

import actor4fun.{Actor, ActorProperties, ActorRef, ActorSystemProperties}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class LocalActorSystem private[actor4fun] (
    override val name: String,
    override val properties: ActorSystemProperties
)(implicit ec: ExecutionContext)
    extends BaseActorSystem(name, properties, ec) {

  override def registerAndManage(name: String, actor: Actor, actorProperties: ActorProperties): ActorRef = {
    if (refs.contains(name))
      throw new IllegalArgumentException(s"$name already registered")

    val actorThread: ActorThread = new ActorThread(name, actor, actorProperties)
    val actorRef: ActorRef       = LocalActorRef(name, this)

    actors.update(actorRef, actorThread)
    refs.update(actorRef.name, actorRef)
    threads.update(
      actorRef,
      Future { actorThread.start(actorRef); actorThread }
    )

    actorRef
  }

  override def shutdown(): Unit = {
    timer.cancel()
    val future = Future.sequence(threads.values.toList)
    actors.values.foreach(_.shutdown())
    Await.ready(
      future,
      Duration(properties.shutdownTimeout._1, properties.shutdownTimeout._2)
    )
    logger.info(s"$name: actor system has shutdown")
  }

  override def awaitTermination(): Unit = {
    val future = Future.sequence(threads.values.toList)

    Await.ready(future, Duration.Inf)
  }
}
