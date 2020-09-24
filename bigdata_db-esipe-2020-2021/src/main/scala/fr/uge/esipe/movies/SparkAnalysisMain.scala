package fr.uge.esipe.movies

import org.apache.spark.sql.SparkSession

object SparkAnalysisMain {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val shootDs =
      spark.read
        .option("header", true)
        .option("sep", ";")
        .csv("data/lieux-de-tournage-a-paris.csv")

    shootDs.createOrReplaceTempView("shoot")
//    val spaceDs =
//      spark.read.option("header", true).option("sep", ";")
//      .csv("data/espaces_verts.csv")

    spark
      .sql("""SELECT * FROM shoot WHERE `Année du tournage` >= 2018""")
      .show()
  }
}
