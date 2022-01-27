package fr.universite_gustave_eiffel.esipe.spark_course

import fr.universite_gustave_eiffel.esipe.spark_course.internal._
import org.apache.spark.sql._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.functions._

import java.time.Instant

/**
 * Join leads to problem that generally can be solved in O(N**2) time,
 * and this is bad.
 *
 * When joining data, you have to consider two sets of data, and try
 * find for each data in the first set, the one that matches in the
 * second set. A naive algorithm would:
 *
 *   1. Take a line in the first dataset.
 *   1. Loop other the lines of the second dataset until we find the
 *      matching one (or not).
 *   1. Repeat with the next line in the first dataset.
 *
 * This is the '''cartesian product'''. But, this is extremely
 * inefficient. And it only works for inner join. For the other kind of
 * joins, you need to apply this algorithm on both datasets separately.
 * So, you get an algorithm complexity of O(2xN^2^), which is closed to
 * O(N^2^).
 *
 * Under Spark SQL, we used to process structured data, on which you may
 * determine a key. A key is supposed to be unique in a dataset. In this
 * case, you can data store data in a hash table (access complexity
 * closes to O(N)), and then reduce the overall complexity to something
 * also more or less closed to O(N). If your data are sorted according
 * to the key, you are even closer to O(N) with just one pass. Then, if
 * you add a sort step, you may have better performances in comparison
 * to the previous approach.
 *
 * But do not forget that you have to deal with low resource machines,
 * in comparison to the volume of the data you have to process. So,
 * whatever the join strategy you choose, the most efficient one is the
 * one that reduces network exchanges and does not lead to OOM error
 * (Out-Of-Memory) on a local machine.
 *
 * In this source file, we will see how Spark SQL works in a view to
 * join data about venues (seen in the previous source file) and checkin
 * data (set of checkins emitted by people).
 */
object joins {

  def main(args: Array[String]): Unit =
    time("Joins") {
      // Let's create a Spark session
      val spark =
        SparkSession
          .builder()
          .appName(getClass.getSimpleName)
          .master("local[*]")
          // The timestamp format in data implies to use this config.
          .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
          .config("spark.eventLog.enabled", true)
          .getOrCreate()

      import spark.implicits._

      val venuesFilename   = "data/threetriangle/venues.txt.gz"
      val checkinsFilename = "data/threetriangle/checkins.txt.gz"

      // First, we load venue data
      val venues =
        spark.read
          .option("sep", "\t")
          .schema("id STRING, latitude DOUBLE, longitude DOUBLE, locationType STRING, country STRING")
          .csv(venuesFilename)
          .as[Venue]
          .cache()

      // Then, we load checkin data
      val checkins =
        spark.read
          .option("sep", "\t")
          // Option to correctly interpret timestamp in checkin data
          .option("timestampFormat", "EEE MMM d HH:mm:ss Z yyyy")
          .schema("userId STRING, venueId STRING, timestamp TIMESTAMP, tzOffset INT")
          .csv(checkinsFilename)
          .as[Checkin]
          .cache()

      exercise("Join") {

        exercise("Join with Spark API", activated = false) {

          /**
           * The `join` method is almost easy to use. It is applied on a
           * first dataframe (considered are the left one) and then, you
           * have to specify the right dataframe. Optionally, you will
           * specify the join constraint and the kind of join (inner,
           * outer, left outer, right outer...).
           */
          val data =
            time("Join with Spark API") {
              checkins.join(venues, checkins("venueId") === venues("id"))
            }

          time("Show: Join with Spark API") {
            data.show()
          }
        }

        exercise("Join with SQL", activated = false) {
          venues.createOrReplaceTempView("VENUES")
          checkins.createOrReplaceTempView("CHECKINS")

          /** You can join in Spark by using the SQL syntax. */
          val data =
            time("Join with SQL") {
              spark.sql("""
                          |SELECT *
                          |FROM CHECKINS c
                          |INNER JOIN VENUES v ON (v.id = c.venueId)
                          |""".stripMargin)
            }

          time("Show: Join with SQL") {
            data.show()
          }
        }

      }

      exercise("Execution plan of a join") {
        exercise("Execution plan", activated = false) {
          val data = checkins.join(venues, checkins("venueId") === venues("id"))

          data.explain(extended = true)

          /**
           * In the physical plan, we can see that Spark indicates
           * `BroadcastExchange` and `BroadcastHashJoin`, this
           * corresponds to one of the four available join strategies.
           * Here is the list:
           *
           *   - BROADCAST / BROADCASTJOIN / MAPJOIN
           *   - MERGE / SHUFFLE_MERGE / MERGEJOIN
           *   - SHUFFLE_HASH
           *   - SHUFFLE_REPLICATE_NL
           *
           * We will describe those join strategies and their usage in
           * the following exercise.
           */
        }
      }

      exercise("Changing join strategy") {

        /**
         * If you want to enforce the broadcast strategy, with the Spark
         * API, you can do
         * {{{
         *   df1.join(df2.hint("BROADCAST"), ...)
         * }}}
         *
         * With SQL, you can do
         * {{{
         *   spark.sql("""SELECT /*+ BROADCAST */ ...""")
         * }}}
         *
         * ==Broadcast Hash Join==
         *
         * This strategy is selected if one of your dataset is small
         * enough to fit in memory. It will be broadcast to all executor
         * of your application, as a hash table. ''Broadcast'' here
         * means that a copy of data is sent to all of the executors,
         * resulting in network transmission although.
         *
         * The threshold that determines if a dataset is small is given
         * by this parameter.
         *
         *   - `spark.sql.autoBroadcastJoinThreshold` in bytes (default:
         *     10485760 (= 10 MB))
         *   - `spark.sql.broadcastTimeout` in seconds (default: 300 (=
         *     5mn)) is used to check if there some communication
         *     problem over the network during broadcast.
         *
         * ==Shuffle Sort-Merge Join==
         *
         * This join strategy performs its tasks in the same order as
         * the title of this section suggests. First, it starts with a
         * '''shuffle''': data are repartitioned and if they have the
         * same key (and same hash), they are sent to the same machine.
         * Then, a sort is applied, and the two datasets are iterated
         * while performing a merge.
         *
         * ==Shuffle Hash Join==
         *
         * This join strategy consists in using the same paritioner to
         * split both datasets and to distribute the equivalent
         * partitions on the same machine.
         *
         * ==Shuffle-and-Replicate Nested Loop Join==
         *
         * This one is used to perform a join based on a cartesian
         * product.
         */

        /**
         * TODO EXERCISE 1: In the 3 following exercises use the 3 other
         * join strategies, not applied. Observe how the execution plan
         * is changed. With the broadcast strategy seen above, determine
         * which strategy is the best?
         */

        exercise("Merge strategy", activated = false) {
          val data_merge: DataFrame =
            time("Merge strategy") {
              ???
            }
          time("Show: Merge strategy") {
            data_merge.show()
          }
          data_merge.explain(true)
        }

        exercise("Shuffle-Hash strategy", activated = false) {
          val data_shuffle: DataFrame =
            time("Shuffle-Hash strategy") {
              ???
            }
          time("Show: Shuffle-Hash strategy") {
            data_shuffle.show()
          }
          data_shuffle.explain(true)
        }

        exercise("Shuffle-Replicate NL strategy", activated = false) {
          val data_srnl: DataFrame =
            time("Shuffle-Replicate NL strategy") {
              ???
            }
          time("Show: Shuffle-Replicate NL strategy") {
            data_srnl.show()
          }
          data_srnl.explain(true)
        }
      }

      exercise("Exploit joined data") {

        /**
         * TODO EXERCISE 2: determine from both the dataset, the
         * top-five of the most popular location type?
         */
        exercise("Top-five location type", activated = false) {
          val data: DataFrame = ??? // join
          val top: DataFrame  = ??? // top-five from the join

          time("Top-five location type") {
            top.show(numRows = 5)
          }
        }
      }
    }
}

case class Checkin(userId: String, venueId: String, timestamp: Instant)
