package fr.uge.esipe.stream

package object log {

  case class ProducerRecord(
      topic: String,
      key: Array[Byte],
      value: Array[Byte]
  )

  case class ConsumerRecord(
      topic: String,
      partitionId: Int,
      offset: Int,
      key: Array[Byte],
      value: Array[Byte]
  )
}
