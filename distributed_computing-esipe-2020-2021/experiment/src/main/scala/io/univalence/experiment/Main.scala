package io.univalence.experiment

import scala.io.Source
import scala.util.Using

object Main {
  def main(args: Array[String]): Unit = {
    Using(Source.fromFile("data/lieux-de-tournage-a-paris.csv")) { file =>
    val result: List[(String, Int)] =
      (for {
        line <- file.getLines()
        if !line.startsWith("Identifiant du lieu")
      } yield {
        val fields = line.split(";")
        Shooting(fields(0), fields(1).toInt, fields(3), fields(6))
      })
        .toList
        .groupBy(_.location)
        .view.mapValues(_.size)
        .toList
        .sortBy(s => -s._2)
        .take(10)

      result.foreach(println)
    }.get
  }
}

case class Shooting(id: String, year: Int, title: String, location: String)
