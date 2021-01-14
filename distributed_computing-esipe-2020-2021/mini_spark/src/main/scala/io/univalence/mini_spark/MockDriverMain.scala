package io.univalence.mini_spark

import actor4fun.ActorSystem
import io.univalence.mini_spark.computation.Driver
import scala.concurrent.ExecutionContext

/**
 * Driver application to test fault-tolerance mechanism. You need to
 * run executors in parallel.
 */
object MockDriverMain {
  val driverHost = "127.0.0.1"
  val driverPort = 19090
  val driverName = "remote-driver"

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem.createRemote(
        s"$driverName-actorsystem",
        driverHost,
        driverPort
      )(ExecutionContext.global)
    sys.addShutdownHook(system.shutdown())

    val driver =
      system.registerAndManage(driverName, new Driver(driverName, system, 2))

    system.awaitTermination()
  }

}
