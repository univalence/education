package io.univalence.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkSqlMain {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("SparkApp")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

//    import org.apache.spark.sql.types._
//    import org.apache.spark.sql.Row

    val sc = spark.sparkContext

    val data = List("Paris,Food,19.00", "Marseille,Clothing,12.00", "Paris,Food,8.00",
      "Paris,Clothing,15.00", "Marseille,Food,20.00", "Lyon,Book,10.00")
      .map(_.split(","))
      .map(r => Depense(r(0), r(1), r(2).toDouble))
//      .map(r => Row(r(0), r(1), r(2).toDouble))

    val rdd: RDD[Depense] = sc.parallelize(data)

//    val schema = StructType(List(
//      StructField("city",  StringType, nullable=true),
//      StructField("type",  StringType, nullable=true),
//      StructField("price", DoubleType, nullable=true)))

//    val df = spark.createDataFrame(sc.parallelize(data), schema)
    val df: Dataset[Depense] = spark.createDataset(rdd)

    val result: DataFrame =
    df
      .select(concat($"ville", lit("_hello")), $"prix")
      .withColumn("50percents", $"prix" / 2.0)

//    result.show()

    df.groupBy($"ville").agg(
      sum($"prix").as("total"),
      collect_set($"type").as("types")
    ).show()
  }
}

case class Depense(ville: String, `type`: String, prix: Double)
