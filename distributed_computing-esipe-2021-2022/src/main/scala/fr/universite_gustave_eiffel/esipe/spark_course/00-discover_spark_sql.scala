package fr.universite_gustave_eiffel.esipe.spark_course

import fr.universite_gustave_eiffel.esipe.spark_course.internal._
import org.apache.spark.sql._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.functions._

/**
 * In this file, you will find supervised exercises to discover '''Spark
 * SQL'''.
 *
 * Spark SQL aims to provide a tool to process structured data typically
 * with a large volume, in a Spark cluster or locally. It is inspired by
 * the Python library Pandas, except that Spark SQL provides the
 * distribution aspect.
 *
 * Spark SQL is one module in the Apache Spark galaxy, along with Spark
 * Core, Spark MLlib (for machine learning), Spark Streaming (for
 * (look-alike) stream processing), or Spark GraphX (for distributed
 * graph processing, like PageRank algorithm, originally developed for
 * Google search engine).
 *
 * Spark SQL is the most used module in Apache Spark by data engineers.
 * It can be used along with many languages: Scala (native
 * implementation), Java (with a poorer implementation), Python (by
 * using the network gateway Py4J), R (another language used by data
 * scientists, communicating with Spark by using an in-house network
 * gateway), and SQL (with an integrated interpreter).
 *
 * @see
 *   https://spark.apache.org/docs/3.2.0/sql-programming-guide.html
 */
object discover_spark_sql {

  def main(args: Array[String]): Unit =
    time("Discover Spark SQL") {

      /**
       * To use Spark, you have to first create a Spark session. A Spark
       * session, is a kind of context, where Spark will begin the
       * communication with the Spark cluster (if one is configured),
       * where we will also configured Spark, and by large, we will
       * initiate the Spark application.
       */
      val spark =
        SparkSession
          .builder()
          /**
           * You have to give a name to your Spark session. This name
           * will be used in logs and in the monitoring interface. It is
           * important enough to retrieve the metrics of your Spark
           * application in certain Spark tools.
           */
          .appName("discover_spark_sql")
          /**
           * You also have to indicate the master node of your Spark
           * cluster. It is a string that can have many forms:
           *
           *   - `spark://<hostname>:<port>`, for standalone mode.
           *   - `yarn` (yes, just `yarn`), if the Spark cluster is
           *     based on Hadoop. In this case, you have to configure
           *     `HADOOP_CONF_DIR` or `YARN_CONF_DIR` to locate the file
           *     with the full configuration to find and describe the
           *     Hadoop cluster.
           *   - `k8s://<hostname>:<port>`, if the Spark cluster is
           *     based Kubernetes.
           *
           * If you only want to run Spark locally, with no Spark
           * cluster, you can specify `local[*]` for the master, like
           * below. The `*` indicates that Spark will use the available
           * CPU cores for its processing. Instead of `*`, you can
           * specify a number, eg. `local[2]` to limit Spark to 2 CPU
           * cores.
           */
          .master("local[*]")
          /**
           * This config will be used later, to write data in parquet
           * format.
           */
          .config("spark.sql.parquet.compression.codec", "gzip")
          .getOrCreate()

      /**
       * Once the Spark session created, we import all the implicit
       * declarations from this session, that will make our daily life
       * easier.
       */
      import spark.implicits._

      /**
       * Here comes the interesting things!
       *
       * The file below comes with compressed TSV data
       * (Tabulation-Separated Data, like CSV, but with tabulation as a
       * separator). Spark SQL is able to handle different kinds of file
       * format compressed or not, binary or not (eg. CSV, JSON, Avro,
       * Parquet, ORC...), and much more. As we are in the big data
       * domain, the file to read might be partition into pieces
       * scattered all other a cluster. Input data might not even been a
       * file. Data can come from a database or message queue. If your
       * data format is not supported, plugins are available, and you
       * can even provide your own implementation.
       *
       * The data in the file below are about different popular
       * locations, with their id, coordinates, their type, and their
       * country.
       */
      val filename = "data/threetriangle/venues.txt.gz"

      /**
       * We will use Spark SQL to read the content of the file. As you
       * can notice, we ask Spark to interpret the content of the file
       * as a CSV content (for Comma-Separated Values).
       *
       * As said above, Spark SQL is able to read many file format, like
       * human-readable formats (CSV, JSON file), binary formats like
       * Avro, binary formats dedicated to partitioned/distributed data
       * like Parquet or ORC, any other kind of data sources, like JDBC
       * sources, Hive, Kafka (with Spark Streaming).
       *
       * The expression below returns a dataframe.
       *
       * A *dataframe* is a structure that acts like a collection, but
       * it does not necessary mount data in memory. A dataframe is
       * essentially ''lazy''. This is very welcome, especially if you
       * have terabytes of data to process. Here, lazy means that you
       * will only do necessary processes according to the context.
       *
       * A dataframe divides data into rows and columns (like in a table
       * in a database). A dataframe comes with a schema: all columns
       * have a name and a type. All data in the dataframe should
       * conform to this schema.
       */
      val dataframe =
        spark.read
          .option("sep", "\t")
//          .option("inferSchema", true)
          .schema("id STRING, latitude DOUBLE, longitude DOUBLE, locationType STRING, country STRING")
          .csv(filename)
          /**
           * The line below is used to emulate a cluster of 4 nodes. It
           * will then force to cut the dataset into 4 separated
           * partitions.
           */
          .repartition(4)
          .as[Venue]

      /**
       * This line below asks Spark to keep the content of the file in
       * memory, so Spark does not have to reload the content for every
       * single actions we are doing on our dataframe.
       */
      dataframe.cache()

      exercise("Show data", activated = true) {

        /**
         * Now, we ask Spark to display a sample of what it has read.
         */
        time("show dataframe") {
          dataframe.show()
        }

        /**
         * We can also ask Spark to display the schema it has determined
         * from what it has read.
         */
        dataframe.printSchema()
      }

      // NOW, RUN THIS PROGRAM!!! (it might take some times)

      // ...
      // ...
      // ...
      // ...

      /**
       * At this stage, you will find the results of `show()` and
       * `printSchema()`.
       *
       * Note the time it takes just to show data.
       *
       * ...
       *
       * Well, the result might not be really convincing... :/
       */

      /**
       * Let's making things better!
       *
       * The first thing we will do is to ask Spark not to use the comma
       * (`,`) as a separator, but to use the tabulation (`\t`) instead.
       *
       * TODO EXERCISE 1: in the file read statement, insert the line
       * `.option("sep", "\t")` and run the program.
       */

      // ...
      // ...
      // ...
      // ...

      /**
       * It should be better. But still! The column name is
       * unintelligible and the type of the coordinate columns does not
       * correspond. We need here to provide a schema.
       *
       * There are two possibilities:
       *
       *   - use the Scala API
       *   - describe the schema in a string
       *
       * We will the second option.
       *
       * TODO EXERCISE 2: in the file read statement, insert the line
       * .schema("id STRING, latitude DOUBLE, longitude DOUBLE,
       * locationType STRING, country STRING") and run the program.
       */

      // ...
      // ...
      // ...
      // ...

      /**
       * This should be far better!
       *
       * We will do one last optional thing, that you can only do in
       * Scala and not in Python. But doing this, it can sometimes make
       * your developer's life easier.
       *
       * If you look at the end of this source file, you will see a case
       * class that matches with the content of the file we are reading.
       * So, what we will do is to turn the resulting dataframe into a
       * `Dataset[Venue]`.
       *
       * TODO EXERCISE 3: at the end of the file read statement, add the
       * line `.as[Venue]` and run the program.
       */

      // ...
      // ...
      // ...
      // ...

      /**
       * It does not change a thing in the output. But things will be
       * easier when we will have to manipulate data with the Spark
       * representation.
       *
       * You also may have notice that it takes more time to collect
       * your data from the file.
       *
       * TODO How would you explain such difference in time?
       */

      exercise("Simple task") {

        /**
         * OK! Let's manipulate this Spark dataframe. First thing first,
         * we will simply count the number of available rows in the data
         * file.
         *
         * TODO EXERCISE 4: activate the block below and run the program
         * Note: to activate, change `activated` to `true`.
         */
        exercise("Count", activated = false) {
          val result4 = time("Count data")(dataframe.count())
          println(s"\t>>> count: $result4")

          check("EXERCISE 4", result4 == 10000L)
        }
      }

      exercise("Simple Query - 1") {

        /**
         * Now, we will display the list of location types.
         *
         * To do so, we will use the Spark API, as below. Every method
         * that appears directly below this comment (`select`,
         * `distinct`), is a method that converts the source dataframe
         * into another dataframe. Those operations are *lazy*: they do
         * not process anything unless you call a method like `count` or
         * `show`, for which you claim a result (those last operations
         * are said to be ''eager'').
         *
         * Notice the use of `$"locationType"` in the `select` method.
         * This is a reference to a column of our dataframe. This
         * notation comes from the statement `import spark.implicits._`
         * in the beginning of this source file. This notation is only
         * available in Scala. There are other notations to reference a
         * column: `col("locationType")` and
         * `dataframe("locationType")`. The last is useful when you have
         * to mix two dataframes, eg. while performing a join.
         *
         * `select` also accepts pure string to reference a column, but
         * in this case, you cannot do complex operations on them, like
         * data transformation, renaming columns, changing their type...
         *
         * TODO EXERCISE 5.1: activate the block below and run the
         * program
         */
        exercise("Query with Spark API", activated = false) {
          val result5_1 =
            time("Query with Spark API") {
              dataframe
                .select($"locationType")
                .distinct()
            }

          time("Show: Query with Spark API") {
            result5_1.show()
          }

          check("EXERCISE 5.1", result5_1.count() == 298L)
        }
      }

      /**
       * The line above is really closed to what you would do with SQL.
       * If you remember, we have seen that we can use SQL with Spark
       * SQL (and the name of this Spark module is not innocent).
       *
       * To do so, we first have to declare a named view from our
       * dataframe. This will act as a SQL table.
       */
      dataframe.createOrReplaceTempView("VENUE")

      exercise("Simple Query - 2") {

        /**
         * Then let's request our dataframe from the created view by
         * using the SQL language.
         *
         * TODO EXERCISE 5.2: activate the block below and run the
         * program
         */
        exercise("Query with SQL", activated = false) {
          val result5_2 =
            time("Query with SQL") {
              spark.sql(
                """
                  |SELECT DISTINCT locationType
                  |FROM VENUE
                  |""".stripMargin
              )
            }
          time("Show: Query with SQL") {
            result5_2.show()
          }
          check("EXERCISE 5.2", result5_2.count() == 298L)
        }
      }

      exercise("GROUP BY") {

        /**
         * We now will count the number of venues for each location
         * type.
         *
         * For this exercise we will need to group the rows according to
         * the locationType and then count the rows for each
         * locationType.
         *
         * TODO EXERCISE 6.1: activate the block below and run the
         * program
         */
        exercise("GROUP BY with Spark API", activated = false) {
          val result6_1 =
            time("GROUP BY with Spark API") {
              dataframe
                .groupBy($"locationType")
                .count()
            }
          time("Show: GROUP BY with Spark API") {
            result6_1.show()
          }

          /**
           * Here, we check the returned dataframe by verifying a part
           * of its content.
           */
          val recordShopCount =
            result6_1
              /**
               * We use a `where` clause to get just on row. Notice the
               * use of triple-equal (`===`) to add constraint and the
               * use `lit` to convert a Scala value into a column
               * expression.
               */
              .where($"locationType" === lit("Record Shop"))
              .head()
              // Then, we extract the count column from the row.
              .getAs[Long]("count")

          // this should display OK in green.
          check("EXERCISE 6.1", recordShopCount == 24L)
        }

        /**
         * TODO EXERCISE 6.2: do the same as the previous exercise, but
         * this time use a SQL expression.
         */
        exercise("GROUP BY with SQL", activated = false) {
          lazy val result6_2 =
            time("GROUP BY with SQL") {
              spark.sql("""
                          |SELECT ???
                          |FROM VENUE
                          |???
                          |""".stripMargin)
            }

          time("Show: GROUP BY with SQL") {
            result6_2.show()
          }

          lazy val recordShopCount =
            result6_2
              .where($"locationType" === lit("Record Shop"))
              .head()
              .getAs[Long]("count")

          check("EXERCISE 6.2", recordShopCount == 24L)
        }
      }

      exercise("Execution plan") {

        /**
         * As you can do SQL Spark SQL, through its API or by directly
         * writing SQL queries in string, like any database, you can ask
         * the system to display the execution plan, in order to
         * understand what Spark will exactly do.
         *
         * TODO EXERCISE 7: activate the block below and run the program
         */
        exercise("Execution plans in Spark SQL", activated = false) {
          val result7 =
            dataframe
              .groupBy($"locationType")
              .count()

          // This line will show you 4 execution plans
          result7.explain(ExtendedMode.name)

          /**
           *   - Parsed Logical Plan: this is what Spark as understood
           *     from your query. It sees an aggregation of data
           *     according to `locationType` by counting them. The data
           *     come from a relation (meaning a dataset or a table)
           *     with the CSV format.
           *   - Analyzed Logical Plan: this the same plan as the
           *     previous one, but Spark has done in addition some type
           *     resolution.
           *   - Optimized Logical Plan: here, Spark cuts the query into
           *     more precise elementary tasks and tries to optimize
           *     them. From the bottom of the plan, Spark will scan the
           *     data file and apply a specific schema. Then the data
           *     are put in memory deserialized (ie. directly
           *     "understandable" by the JVM), and a projection on the
           *     column locationType is done (meaning that the other
           *     columns are removed). Finally, an aggregation will be
           *     done.
           *   - Physical Plan: this is the final phase of the plan
           *     optimization done by Spark. Here, Sparks adapt the plan
           *     to the physical data support (which depends especially
           *     on the file format).
           *
           * Let's take a closer look at the physical plan. In this
           * plan, you can see in particular that after Spark has loaded
           * data in memory, it will two HashAggregates separated by a
           * data Exchange.
           *
           * Especially the data exchange may sound weird, especially if
           * your are on a single machine. But this makes sense when
           * your are processing data distributed on a whole cluster.
           *
           * As an example, let's suppose that the rows for the
           * locationType "Record Shop" are located in two node N1 and
           * N2. What Spark will do according to our query, it will
           * partially count the quantity of row with `locationType ==
           * "Record Shop"` on N1 and on N2 separately. Then, all the
           * partial count are sent to the same node (say N1), where
           * Spark will finalized the process by adding the partial
           * counts to get the total count.
           */
        }
      }

      /**
       * A last thing to do, is to write down data somewhere, so they
       * can be used by other jobs.
       */
      exercise("Write dataframe in file") {
        val outputname = "data/output/venues"

        /**
         * First, we will save data in CSV file.
         *
         * TODO EXERCISE 8.1: activate the block below and run the
         * program
         */
        exercise("Write into CSV file", activated = false) {
          val csvFilename = outputname + ".csv"

          /**
           * As in big data, it is better to respect the principle of
           * immutability, Spark throws an exception if you write the
           * same again. So, we first clean the workspace.
           */
          clean(csvFilename)

          /**
           * You may notice something weird once the file is written: it
           * is not a file, but a directory, that contains many files.
           *
           * First, for every written file in this directory, there is a
           * CRC file, that contains a checksum. Then there is an empty
           * `_SUCCESS` file created once the write job is done.
           *
           * And then, you have series of files with a name that looks
           * like `part-<partition_number>-<uuid>-c000.csv`. This tends
           * to reveal how Sparks work.
           *
           * When you process data in a Spark application, Spark will
           * divide your application into *jobs*. A job is delimited by
           * a method of type action, like `show`, `count`, or `write`.
           * They are eager operations, ie. for which a result is
           * immediately needed. The jobs are then cut into elementary
           * '''tasks'''. All the tasks and their dependencies generate
           * a '''DAG''' (for Direct Acyclic Graph). This graph helps
           * Spark to manage, distribute, and schedule tasks among the
           * nodes of the Spark cluster. If your data are divided into
           * partitions, the DAG is rerun as is for each partition in
           * separated processes or separated threads.
           *
           * This include the write operation. As we have 4 partitions,
           * Spark will convert the write command into 4 write
           * operations (one for each partition). That is why, your data
           * is written in 4 different files.
           *
           * If you want just one file, you have to change the number of
           * partitions to 1, with operations like `repartition` or
           * `coalesce`.
           */
          time("Write into CSV file") {
            dataframe.write.csv(csvFilename)
          }

          /**
           * Notice that Spark has no problem to read data from such
           * file organization. By doing
           * `spark.read.csv("data/output/venues.csv")`, you will
           * retrieve your data.
           */
        }

        /**
         * Next step: write data into Parquet.
         *
         * TODO EXERCISE 8.2: activate the block below and run the
         * program
         */
        exercise("Write into Parquet file", activated = false) {
          val parquetFilename = outputname + ".parquet"

          clean(parquetFilename)

          /**
           * Parquet is a columnar binary file format: the data are
           * organized by column first, instead of being organized by
           * row, like many file format. This is almost efficient when
           * query some specific columns only.
           *
           * Parquet proposes an efficient approach for compressed data.
           * As we have at the beginning of this file, we have
           * configured Spark to apply the GZip codec to Parquet files.
           * Their other compression codec available (uncompressed,
           * snappy, gzip, lzo, brotli, lz4, zstd).
           */
          time("Write into Parquet file") {
            dataframe.write.parquet(parquetFilename)
          }
        }
      }
    }

}

case class Venue(id: String, latitude: Double, longitude: Double, locationType: String, country: String)
