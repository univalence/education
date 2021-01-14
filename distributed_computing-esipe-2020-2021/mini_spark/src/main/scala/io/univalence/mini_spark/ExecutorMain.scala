package io.univalence.mini_spark

import actor4fun.internal.RemoteActorRef
import actor4fun.{ActorSystem, ActorSystemProperties}
import io.univalence.mini_spark.MiniSparkApplicationMain.{driverHost, driverName, driverPort}
import io.univalence.mini_spark.computation.Executor
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext

/**
 * Executor application.
 *
 * This application takes a parameter which is the executor ID (it
 * must an integer).
 */
object ExecutorMain {
  val host     = "127.0.0.1"
  val basePort = 19090

  def main(args: Array[String]): Unit = {
    val id   = args(0).toInt
    val port = basePort + id

    val system =
      ActorSystem.createRemote(
        s"executor-$id-actorsystem",
        host,
        port,
        ActorSystemProperties(
          shutdownTimeout = (5, TimeUnit.SECONDS)
        )
      )(ExecutionContext.global)
    sys.addShutdownHook(system.shutdown())

    // get the reference of the (remote) driver actor
    val driver = RemoteActorRef
      .fromURI(s"actor://$driverHost:$driverPort/$driverName", system)
      .get

    val executor =
      system.registerAndManage(
        s"executor-$id",
        new Executor(id.toString, driver, system)
      )

    system.awaitTermination()
  }

}
