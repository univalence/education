package io.univalence.mini_spark.computation

/**
 * Set of messages used by Mini-Spark
 */
object message {

  case class Heartbeat(id: String)

  case class ExecutorRegister(id: String)

  case class ExecutorRegisterReply(idOrError: Either[String, String])

  case class ExecutorRegisterAgain(id: String)

  case class ExecutorTask(task: TaskContext[Any])

  case class ExecutorTaskResult(taskId: String, results: RawTaskResult[Any])

  case object PoisonPill

}
