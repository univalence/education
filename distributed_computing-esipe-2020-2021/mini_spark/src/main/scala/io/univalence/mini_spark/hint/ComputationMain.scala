package io.univalence.mini_spark.hint

import scala.io.Source
import scala.util.Using

object ComputationMain {
  def main(args: Array[String]): Unit = {
    val filename = "data/lieux-de-tournage-a-paris.csv"
    Using(Source.fromFile(filename)) { file =>
      val computation =
        fromText(file.getLines().toList)
          .filter(l => !l.startsWith("Identifiant du lieu"))
          .map(_.split(";").toList)
          .map(f => Shooting(f(0), f(1).toInt, f(3), f(6)))
          .groupBy(_.year)
          .mapValues(_.size)

      println(computation.collect)
      println(computation.count)
    }.get
  }

  case class Shooting(id: String, year: Int, title: String, address: String)

  sealed trait Computation[A] {
    def map[B](f: A => B): Computation[B]         = MapComp(this, f)
    def filter(p: A => Boolean): Computation[A]   = FilterComp(this, p)
    def groupBy[K](fk: A => K): GroupByComp[K, A] = GroupByComp(this, fk)

    def collect: List[A] = eval(this).asInstanceOf[List[A]]
    def count: Int       = eval(this).size
  }

  case class DataComp[A](data: List[A]) extends Computation[A]
  case class MapComp[A, B](prev: Computation[A], f: A => B)
      extends Computation[B]
  case class FilterComp[A](prev: Computation[A], p: A => Boolean)
      extends Computation[A]
  case class GroupByComp[K, A](prev: Computation[A], fk: A => K)
      extends Computation[(K, List[A])] {
    def mapValues[B](f: List[A] => B): Computation[(K, B)] =
      MapComp[(K, List[A]), (K, B)](this, { case (k, as) => k -> f(as) })
  }

  def fromText(lines: List[String]): DataComp[String] = DataComp(lines)

  // note: this not stack safe
  def eval[A](computation: Computation[A]): List[Any] =
    computation match {
      case DataComp(data)                     => data
      case FilterComp(c, p: (Any => Boolean)) => eval(c).filter(p)
      case MapComp(c, f: (Any => Any))        => eval(c).map(f)
      case GroupByComp(c, fk: (Any => Any))   => eval(c).groupBy(fk).toList
    }

}
