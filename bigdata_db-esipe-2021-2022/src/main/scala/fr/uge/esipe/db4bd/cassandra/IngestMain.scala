package fr.uge.esipe.db4bd.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import fr.uge.esipe.db4bd.model.Venue
import java.net.InetSocketAddress
import scala.io.Source
import scala.util.Using

object IngestMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val venueFilename = "data/threetriangle/venues.txt"

    val session =
      CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withLocalDatacenter("datacenter1")
        .build()

    try {
      createTable(session)

      val start = System.nanoTime()
      loadVenues(venueFilename, session)
      val end = System.nanoTime()
      println((end - start) / 1e-9)
    } finally {
      session.close()
    }
  }

  private def loadVenues(venueFilename: String, session: CqlSession): Unit = {
    Using(Source.fromFile(venueFilename)) { file =>
      file
        .getLines()
        //          .take(1000)
        .foreach { line =>
          val data = line.trim.split("\t")
          val venue = Venue(
            venueId = data(0),
            latitude = data(1).toDouble,
            longitude = data(2).toDouble,
            category = data(3),
            country = data(4)
          )

          session.execute(
            s"""INSERT INTO threetriangle.venue (venue_id, latitude, longitude, category, country)
               VALUES ('${venue.venueId}', ${venue.latitude}, ${venue.longitude}, '${venue.category}', '${venue.country}')
""")
          //            db.put(venue.venueId.getBytes(StandardCharsets.UTF_8), venue.serialize)
        }
    }.get
  }

  private def createTable(session: CqlSession) = {
    session
      .execute(
        """CREATE KEYSPACE IF NOT EXISTS threetriangle
          |  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }
          |  """.stripMargin
      )
      .asScala

    session.execute(
      """CREATE TABLE IF NOT EXISTS threetriangle.venue (
        |  venue_id text,
        |  latitude double,
        |  longitude double,
        |  category text,
        |  country text,
        |  PRIMARY KEY (venue_id)
        |)
        |""".stripMargin)

    session.execute(
      """CREATE TABLE IF NOT EXISTS threetriangle.checkin (
        |  user_id text,
        |  venue_id text,
        |  timestamp timestamp,
        |  offset int,
        |  PRIMARY KEY ((user_id, venue_id), timestamp)
        |) WITH CLUSTERING ORDER BY (timestamp DESC)
        |""".stripMargin)
  }

}
