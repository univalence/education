package fr.universite_gustave_eiffel.esipe.spark_course.course

// GADT (Generalized ADT)
// traitement sur des collections
sealed trait Computation[+A] {

  def map[B](f: A => B): Computation[B] = MapComputation(this, f)

  def filter(pred: A => Boolean): Computation[A] = FilterComputation(this, pred)

}
case class CollectionComputation[A](iterable: Iterable[A]) extends Computation[A]
case class MapComputation[A, B](computation: Computation[A], f: A => B) extends Computation[B]
case class FilterComputation[A](computation: Computation[A], pred: A => Boolean) extends Computation[A]

object Computation {
  def fromCollection[A](iterable: Iterable[A]): Computation[A] = CollectionComputation(iterable)

  def run[A](computation: Computation[A]): Iterable[Any] =
    computation match {
      case CollectionComputation(iterable) => iterable
      case MapComputation(computation, f: (Any => Any)) => run(computation).map(f)
      case FilterComputation(computation, pred: (Any => Boolean)) => run(computation).filter(pred)
    }
}

object ComputationMain {

  def main(args: Array[String]): Unit = {
    val data = (1 to 100).toList
    val computation: Computation[Int] = Computation.fromCollection(data)

    println(Computation.run(computation.map(n => n * n)))
    println(Computation.run(computation.filter(n => n % 2 == 0)))
    println(Computation.run(computation.map(n => "#" + n.toString)))
  }

}
