package io.univalence.mini_spark

import io.univalence.mini_spark.computation.RemoteJobContext
import io.univalence.mini_spark.rdd.RDD

/**
 * Example of analytics application using the Mini-Spark framework.
 */
object MiniSparkApplicationMain {
  val driverHost = "127.0.0.1"
  val driverPort = 19090
  val driverName = "remote-driver"

  val executorCount = 2

  def main(args: Array[String]): Unit = {
    val filepath = "data/split-file/"

    // job context creation (includes the driver actor setup)
    val context = new RemoteJobContext(driverHost, driverPort, driverName, executorCount)
    context.driver.executorCountdownLatch.await()

    // get data from a data source
    //  - from a single file that will be partitioned
//    val data: RDD[String] = context.readTextFile(filepath, partitionCount = 5)
    //  - from a already partitioned file
    val data: RDD[String] = context.readSplitTextFile(filepath)

    // apply transformations to the dataset
    val shoots: RDD[Shooting] =
      data
        .filter(line => !line.startsWith("Identifiant du lieu"))
        .map { line =>
          val fields = line.split(";")

          Shooting(fields(0), fields(3), fields(4), fields(7), fields(1).toInt)
        }

    // Action 1: display all the data.
    shoots.collect.foreach(println)

    // Action 2: group data by year.
    //   (it does not work as expected. Guess why?)
//    shoots.groupBy(_.year).collect.foreach(println)
    println(s"count: ${shoots.count}")

    // Action 3: produce another partitioned file from the transformed
    //   dataset.
    shoots
      .map(shoot => s"${shoot.id},${shoot.title},${shoot.director},${shoot.year},${shoot.postcode}")
      .saveTextFile("data/out/out.csv")

    context.stop()
  }

}

case class Shooting(
    id: String,
    title: String,
    director: String,
    postcode: String,
    year: Int
)
