package fr.uge.esipe.student

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.io.Source
import scala.util.{Failure, Success, Try}

trait StudentKeyValueStore {
  def insert(student: Student): Try[Unit]

  def find(id: String): Try[Student]
}

import scala.collection.mutable

class InMemoryStudentKeyValueStore extends StudentKeyValueStore {
  val data: mutable.Map[String, Student] = mutable.Map.empty

  override def insert(student: Student): Try[Unit] =
    Try(data += (student.id -> student))

  override def find(id: String): Try[Student] =
    Try(data(id))
}

class IndexedFileStudentKeyValueStore(directory: String) extends StudentKeyValueStore {
  val path: Path = Paths.get(directory)
  val indexPath: Path = path.resolve("index.data")
  val filePath: Path = path.resolve("content.data")

  def init(): Unit = {
    if (!path.toFile.exists()) {
      Files.createDirectories(path)
    }
    if (!filePath.toFile.exists()) {
      Files.createFile(filePath)
    }
    if (!indexPath.toFile.exists()) {
      Files.createFile(indexPath)
    }
  }
  override def insert(student: Student): Try[Unit] = {
    val indexes = readIndex

    if (indexes.contains(student.id))
      Failure(new IllegalArgumentException(s"${student.id} already exists"))
    else {
      val file = FileChannel.open(filePath, StandardOpenOption.APPEND)
      try {
        val data = s"${student.id},${student.name},${student.age}\n"
        val position = file.position()
        file.write(ByteBuffer.wrap(data.getBytes))

        val newIndexes = indexes.updated(student.id, position)
        writeIndex(newIndexes)
      } finally {
        file.close()
      }
    }
  }

  override def find(id: String): Try[Student] = {
    val indexes = readIndex

    Try(indexes(id)).map { position =>
      val file = Files.newBufferedReader(filePath)
      try {
        file.skip(position)
        val line = file.readLine()
        val fields = line.split(",")

        Student(id = fields(0), name = fields(1), age = fields(2).toInt)
      } finally {
        file.close()
      }
    }
  }

  private def writeIndex(data: Map[String, Long]): Try[Unit] = {
    val file = Files.newBufferedWriter(indexPath)
    try {
      for ((k, v) <- data) {
        file.write(s"${k},${v}\n")
      }

      Success(())
    } catch {
      case e: Exception => Failure(e)
    } finally {
      file.close()
    }
  }

  private def readIndex: Map[String, Long] = {
    val file = Source.fromFile(indexPath.toFile)

    val data: List[(String, Long)] =
      try {
        file.getLines().map { line =>
          val fields = line.split(",")

          (fields(0) -> fields(1).toLong)
        }.toList
      } finally {
        file.close()
      }

    data.groupBy(_._1).mapValues(_.head._2)
  }

  //  case class Index(id: String, position: Long)
}

class FileStudentKeyValueStore(directory: String) extends StudentKeyValueStore {
  val path: Path = Paths.get(directory)
  val filePath: Path = path.resolve("content.data")

  def init(): Unit = {
    if (!path.toFile.exists()) {
      Files.createDirectories(path)
    }
    if (!filePath.toFile.exists()) {
      Files.createFile(filePath)
    }
  }

  override def insert(student: Student): Try[Unit] = {
    val data = readData

    if (data.contains(student.id))
      Failure(new IllegalArgumentException(s"${student.id} already exists"))
    else {
      val newData = data.updated(student.id, student)

      writeData(newData)
    }
  }

  private def writeData(data: Map[String, Student]): Try[Unit] = {
    val file = Files.newBufferedWriter(filePath)
    try {
      for ((k, v) <- data) {
        file.write(s"${k},${v.name},${v.age}\n")
      }

      Success(())
    } catch {
      case e: Exception => Failure(e)
    } finally {
      file.close()
    }
  }

  private def readData: Map[String, Student] = {
    val file = Source.fromFile(filePath.toFile)

    val studentData: List[Student] =
      try {
        file.getLines().map { line =>
          val fields = line.split(",")

          Student(id = fields(0), name = fields(1), age = fields(2).toInt)
        }.toList
      } finally {
        file.close()
      }

    studentData.groupBy(_.id).mapValues(_.head)
  }

  override def find(id: String): Try[Student] = {
    Try(readData(id))
  }
}

case class Student(
                    id: String,
                    name: String,
                    age: Int
                  )
