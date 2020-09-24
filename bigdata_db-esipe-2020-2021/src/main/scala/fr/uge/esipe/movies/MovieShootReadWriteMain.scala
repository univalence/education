package fr.uge.esipe.movies

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.io.{BufferedSource, Source}
import scala.util.hashing.MurmurHash3

object MovieShootReadWriteMain {

  def main(args: Array[String]): Unit = {
    val data = read("data/lieux-de-tournage-a-paris.csv")
    val directories = List("output/target1", "output/target2", "output/target3")
    write(data, directories)
  }

  def write(data: List[MovieShoot], directories: List[String]): Unit = {
    directories.map(d => Files.createDirectories(Paths.get(d)))

    data
      .foreach { shoot =>
        val index = MurmurHash3.stringHash(shoot.id).abs % directories.length
        val directory = directories(index)

        val writer =
          Files.newBufferedWriter(
            Paths.get(directory, s"block.data"),
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE
          )
        try {
          val line =
            s"${shoot.id};${shoot.year};${shoot.movieType};${shoot.title};${shoot.director};${shoot.producer};${shoot.location};${shoot.postCode}"
          writer.write(line + "\n")
        } finally {
          writer.close()
        }
      }
  }

  def read(filename: String): List[MovieShoot] = {
    val file = Source.fromFile(filename)
    try {
      transformLines(file).toList
    } finally {
      file.close()
    }
  }

  private def transformLines(file: BufferedSource): Iterator[MovieShoot] = {
    file
      .getLines()
      .drop(1)
      .map { l =>
        val fields = l.split(";").toList

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
  }
}
