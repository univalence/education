package io.univalence.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkContext
        .getOrCreate(
          new SparkConf()
            .setAppName("xp")
            .setMaster("local[*]")
        )

    val rdd: RDD[String] =
      spark.textFile("data/lieux-de-tournage-a-paris.csv")
//    val rdd: RDD[String] = spark.textFile("data/split-file")

    val result: RDD[(String, Int)] =
      rdd
        .filter(l => !l.startsWith("Identifiant du lieu"))
        .map { line =>
          val fields = line.split(";")
          Shooting(fields(0), fields(1).toInt, fields(3), fields(6))
        }
        .groupBy(_.location)
        .mapValues(_.size)
        .sortBy(-_._2)

    result.take(10).toList.foreach(println)
  }

}

case class Shooting(id: String, year: Int, title: String, location: String)
