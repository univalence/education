package io.univalence.spark_core

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object TransactionMain {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("SparkApp")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val df = (
      Seq(
        ("Customer A", "2020-08-07", 1249),
        ("Customer B", "2020-09-02", 915),
        ("Customer B", "2020-09-27", 2289),
        ("Customer A", "2020-10-01", 1343),
        ("Customer C", "2020-10-01", 2099),
        ("Customer A", "2020-10-04", 329),
        ("Customer A", "2020-10-08", 638)
      ).toDF("customer_id", "transaction_date", "amount")
        .withColumn(
          "transaction_date",
          $"transaction_date".cast("date")
        )
      )

    val customerWindow = Window.partitionBy("customer_id")
    val customerMonthWindow = Window.partitionBy($"customer_id", month($"transaction_date"))

    (df
      .withColumn("transac_num", rank().over(customerWindow.orderBy("transaction_date")))
      .withColumn("transac_lst_30d", count("*").over(customerWindow.orderBy(unix_timestamp($"transaction_date")).rangeBetween(-2592000, 0)))
      .withColumn("nb_transac_in_month", count("*").over(customerMonthWindow))
      ).sort("transaction_date").show(truncate=false)
  }
}
