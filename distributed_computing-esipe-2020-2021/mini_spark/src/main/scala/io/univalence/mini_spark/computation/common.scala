package io.univalence.mini_spark.computation

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

/**
 * Include functions to serialize lambda expressions.
 */
object common {

  def serializeLambda[A, B](f: A => B): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val os  = new ObjectOutputStream(bos)
    os.writeObject(f)
    val fData = bos.toByteArray
    bos.close()

    fData
  }

  def deserializeLambda(fData: Array[Byte]): AnyRef => AnyRef = {
    val bin = new ByteArrayInputStream(fData)
    val in  = new ObjectInputStream(bin)

    val f = in.readObject().asInstanceOf[AnyRef => AnyRef]
    bin.close()

    f
  }

}
