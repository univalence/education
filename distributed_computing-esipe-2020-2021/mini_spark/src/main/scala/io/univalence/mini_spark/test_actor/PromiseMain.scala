package io.univalence.mini_spark.test_actor

import scala.concurrent.Promise

object PromiseMain {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._

    val promise = Promise[Int]

    val f = promise.future

    f.foreach(println)

    Thread.sleep(500)

    promise.success(42)
  }
}
