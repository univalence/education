package fr.uge.esipe.movies

import scala.io.{BufferedSource, Source}

object MovieShootAnalysisMain {

  def main(args: Array[String]): Unit = {
    val data = read("data/lieux-de-tournage-a-paris.csv", lineToMovieShoot)

    println(s"count of movie shoots: ${data.length}")
    println(s"max year: ${data.map(_.year).max}")
    println(s"min year: ${data.map(_.year).min}")

    //    data
    //      .groupBy(_.title)
    //      .view
    //      .mapValues(_.size)
    //      .toList
    //      .sortBy { case (_, count) => -count }
    //      .take(10)
    //      .foreach(titleCount => println(titleCount))

    val spaceData = read("data/espaces_verts.csv", lineToGreenSpace)

    val shootByStreet: Map[String, List[MovieShoot]] =
      data
        .groupBy { shoot =>
          shoot.location.split(",", 2).head.toUpperCase
        }

    val spaceByStreet: Map[String, List[GreenSpace]] =
      spaceData
        .groupBy { space =>
          s"${space.streetType} ${space.streetName}".toUpperCase
        }

    val cogroup: Map[String, (List[MovieShoot], List[GreenSpace])] = {
      // Map<Sting, Tuple2<List<...>, List<...>>>
      shootByStreet.view.map {
        case (street, shoot) =>
          street -> (shoot, spaceByStreet.getOrElse(street, List()))
      }.toMap
    }

    val result: Map[String, Int] =
      cogroup
        .filter { case (_, (_, spaces)) => spaces.nonEmpty }
        .map { case (street, (shoots, _)) => street -> shoots.size }

    result.toList
      .sortBy { case (_, count) => -count }
      .take(10)
      .foreach(println)
  }

  def read[A](filename: String, transformLine: List[String] => A): List[A] = {
    val file = Source.fromFile(filename)
    try {
      transformLines(file, transformLine).toList
    } finally {
      file.close()
    }
  }

  private def transformLines[A](
      file: BufferedSource,
      transformLine: List[String] => A
  ): Iterator[A] = {
    file
      .getLines()
      .drop(1)
      .map(_.split(";").toList)
      .map(transformLine)
  }

  private def lineToMovieShoot(fields: List[String]): MovieShoot = {
    MovieShoot(
      id = fields(0),
      year = fields(1).toInt,
      movieType = fields(2),
      title = fields(3),
      director = fields(4),
      producer = fields(5),
      location = fields(6),
      postCode = fields(7)
    )
  }

  private def lineToGreenSpace(fields: List[String]): GreenSpace =
    GreenSpace(
      id = fields(0),
      name = fields(1),
      spaceType = fields(2),
      category = fields(3),
      streetType = fields(6),
      streetName = fields(7),
      postCode = fields(8)
    )
}

case class GreenSpace(
    id: String,
    name: String,
    spaceType: String,
    category: String,
    streetType: String,
    streetName: String,
    postCode: String
)
