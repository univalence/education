package io.univalence.mini_spark.computation

import io.univalence.mini_spark.rdd.{Partition, RDD}

case class TaskContext[A](
    id: String,
    partition: Partition,
    rdd: RDD[A]
)

/**
  * Task result representation, easier to manipulate.
  */
case class TaskResult[A](
    id: String,
    context: TaskContext[A],
    data: Seq[A]
)

/**
  * Task result representation, easier to serialized.
  */
case class RawTaskResult[A](
    id: String,
    context: TaskContext[A],
    data: Array[A]
) {
  def toAbstract: TaskResult[A] =
    TaskResult(id, context, data.toSeq)
}
