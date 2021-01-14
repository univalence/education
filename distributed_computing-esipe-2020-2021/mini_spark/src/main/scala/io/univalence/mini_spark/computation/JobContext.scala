package io.univalence.mini_spark.computation

import actor4fun.{ActorRef, ActorSystem}
import io.univalence.mini_spark.computation.message._
import io.univalence.mini_spark.rdd.RDD
import io.univalence.mini_spark.rdd.RDD._
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Manage Mini-Spark application, including driver/executors settings.
  *
  * JobContext is actually the actor system.
  */
trait JobContext {
  def readTextFile(filepath: String): RDD[String]
  def runJob[A, B](rdd: RDD[A], postProcess: Seq[TaskResult[A]] => B): B
  def runJob[A](rdd: RDD[A]): Seq[A] =
    runJob(rdd, (x: Seq[TaskResult[A]]) => x.flatMap(_.data))
}
object JobContext {

  def create(
      host: String,
      port: Int,
      name: String,
      executorCount: Int
  ): JobContext =
    new RemoteJobContext(host, port, name, executorCount)

}

/**
  * JobContext implementation implying remote communication between
  * driver and executors.
  *
  * note: `executorCount` indicates the number of executors to wait
  * before running the computation.
  */
class RemoteJobContext(
    host: String,
    port: Int,
    name: String,
    executorCount: Int
) extends JobContext {
  val system: ActorSystem =
    ActorSystem.createRemote(name + "-system", host, port)(
      ExecutionContext.global
    )

  locally {
    // automatically call shutdown when the JVM stops.
    sys.addShutdownHook(system.shutdown())
  }

  val driver: Driver      = new Driver(name, system, executorCount)
  val driverRef: ActorRef = system.registerAndManage(name, driver)

  val taskIdGenerator = new AtomicInteger(0)

  /**
    * Convert a Seq (List) into a RDD.
    */
  def parallelize[A](seq: Seq[A]): RDD[A] =
    SeqRDD(seq, driver.executors.size, this)

  def readTextFile(filepath: String): RDD[String] =
    TextFileRDD(filepath, driver.executors.size, this)

  def readTextFile(filepath: String, partitionCount: Int): RDD[String] =
    TextFileRDD(filepath, partitionCount, this)

  def readSplitTextFile(filepath: String): RDD[String] =
    SplitTextFileRDD(filepath, this)

  /**
    * Run a job represented by a RDD and apply a post-processing
    * function to the fetched results.
    */
  override def runJob[A, B](
      rdd: RDD[A],
      postProcess: Seq[TaskResult[A]] => B
  ): B = {
    // create one task for each partition
    val tasks: Seq[TaskContext[A]] =
      rdd.partitions.map { p =>
        TaskContext(
          id = taskIdGenerator.getAndIncrement().toString,
          partition = p,
          rdd = rdd.toLocal
        )
      }

    // register all tasks on the driver
    val futures: Seq[Future[RawTaskResult[A]]] =
      tasks.map { task => driver.registerTask(task).future }

    // send tasks to the executors
    //   We apply a round-robin algorithm here. If there more tasks than
    //   there are executors, then the remaining tasks are redistributed
    //   to the available executors.
    tasks.zipWithIndex.foreach {
      case (task, idx) =>
        // idx here is used to distribute uniformly the tasks among
        // the available executors.
        val (_, ref) = driver.executors.toList(idx % driver.executors.size)

        println(s"${getClass.getSimpleName}: sending $task to $ref")

        ref.sendFrom(
          driverRef,
          ExecutorTask(task.asInstanceOf[TaskContext[Any]])
        )
    }

    // wait for all the tasks to finish
    val result: Seq[RawTaskResult[A]] =
      futures.map(f => Await.result(f, Duration.Inf))

    // remove all the registered tasks from the driver
    tasks.foreach(task => driver.removeTask(task.id))

    // apply the post-processing function and return its result
    postProcess(result.map(_.toAbstract))
  }

  def awaitTermination(): Unit = system.awaitTermination()

  def stop(): Unit = system.shutdown()
}

/**
  * JobContext implementation implying local communication between
  * driver and executors, being on the same machine.
  */
class LocalJobContext() extends JobContext with Serializable {
  def readTextFile(filepath: String): RDD[String] =
    TextFileRDD(filepath, 2, this)

  def runJob[A, B](rdd: RDD[A], postProcess: Seq[TaskResult[A]] => B): B = {
    val tasks: Seq[TaskContext[A]] =
      rdd.partitions.map { p =>
        TaskContext(
          id = UUID.randomUUID().toString,
          partition = p,
          rdd = rdd
        )
      }

    val result =
      tasks.map { t =>
        val results = t.rdd.compute(t.partition)

        TaskResult(
          id = t.id,
          context = t,
          data = results.toSeq
        )
      }

    postProcess(result)
  }

}

object LocalJobContext {
  def create: LocalJobContext = new LocalJobContext()
}
