package fr.universite_gustave_eiffel.esipe.spark_course

import org.apache.spark.sql.Column

import scala.util.{Failure, Success, Try}

import java.io.File
import java.time.LocalDateTime

package object internal {

  def ?? : Column = ???

  final private case class ActivateContext(label: String)
  private var activatedContexts: Seq[ActivateContext] = Seq.empty

  def exercise(label: String, activated: Boolean)(f: => Unit): Unit =
    if (activated) {
      activatedContexts = activatedContexts :+ ActivateContext(label)
      part(activatedContexts.map(_.label).mkString(" > "))

      try f
      catch {
        case PartException(l, cause) =>
          throw PartException(s"$label > $l", cause)
        case e: Exception =>
          throw PartException(label, e)
      } finally activatedContexts = activatedContexts.init
    } else ()

  def exercise(label: String)(f: => Unit): Unit = exercise(label, activated = true)(f)

  case class PartException(label: String, cause: Throwable)
      extends RuntimeException(s"""Exception caught in part "$label"""", cause)

  def part(label: String): Unit = println(s"${Console.YELLOW}+++ PART $label${Console.RESET}")

  def check(label: String, condition: => Boolean): Unit = {
    val result = Try(condition)

    val display =
      result match {
        case Success(true)  => s"${Console.GREEN}OK${Console.RESET}"
        case Success(false) => s"${Console.YELLOW}FAILED${Console.RESET}"
        case Failure(e)     => s"${Console.RED}ERROR: ${e.getClass.getCanonicalName}: ${e.getMessage}${Console.RESET}"
      }

    println(partIndent + s">>> $label: $display")
  }

  private def partIndent: String = "\t" * activatedContexts.size

  def clean(outputFilename: String): Unit = {
    val file = new File(outputFilename)

    if (file.exists()) {
      if (file.isDirectory) {
        file.listFiles().toList.foreach(_.delete())
        file.delete()
      } else {
        file.delete()
      }
    }
  }

  def time[A](label: String)(f: => A): A = {
    val start = java.lang.System.nanoTime()
    println(partIndent + s""">>> ${Console.YELLOW}Start "$label" @ ${LocalDateTime.now}${Console.RESET}""")
    try f
    finally {
      val end = java.lang.System.nanoTime()
      println(partIndent + s""">>> ${Console.YELLOW}End "$label" @ ${LocalDateTime.now}${Console.RESET}""")
      val delta = (end - start) * 1e-9
      println(
        partIndent + s""">>> ${Console.YELLOW}Elapsed time for "$label": ${Console.RED}${displayTime(
          delta
        )}${Console.RESET}"""
      )
    }
  }

  private def displayTime(seconds: Double): String = {
    def round2dec(value: Double): Double = Math.round(value * 100.0) / 100.0

    val posSeconds = Math.abs(seconds)
    lazy val order = Math.floor(Math.log10(posSeconds))
    if (posSeconds < 1e-9) "0s"
    else if (posSeconds >= 60.0 * 60.0) {
      val hour       = (seconds / (60.0 * 60.0)).toInt
      val minutes    = ((seconds / 60.0) - hour * 60.0).toInt
      val remSeconds = round2dec((seconds / 60.0 - (hour * 60.0 + minutes)) * 60.0)
      s"${hour}h${minutes}m${remSeconds}s"
    } else if (posSeconds >= 60.0 && posSeconds < 60.0 * 60.0) {
      val minutes    = (seconds / 60.0).toInt
      val remSeconds = round2dec((seconds / 60.0 - minutes) * 60.0)
      s"${minutes}m${remSeconds}s"
    } else if (order < 0 && order >= -3.0) {
      s"${round2dec(seconds * 1e3)}ms"
    } else if (order < -3 && order >= -6.0) {
      s"${round2dec(seconds * 1e6)}µs"
    } else if (order < -6 && order >= -9.0) {
      s"${round2dec(seconds * 1e9)}ns"
    } else {
      s"${round2dec(seconds)}s"
    }
  }

}
