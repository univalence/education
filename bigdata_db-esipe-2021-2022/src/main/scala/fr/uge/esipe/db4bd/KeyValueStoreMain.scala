package fr.uge.esipe.db4bd

import fr.uge.esipe.db4bd.model.{Checkin, Venue}
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Using

object KeyValueStoreMain {

  def main(args: Array[String]): Unit = {
    val filename =
      "data/threetriangle/checkins.txt"

    val db = new TreeKeyValueStore[(String, LocalDateTime), Checkin]

    Using(Source.fromFile(filename)) { file =>
      file
        .getLines()
        .take(100000)
        .foreach { line =>
          val data = line.trim.split("\t")
          val checkin = Checkin(
            userId = data(0),
            venueId = data(1),
            // Tue Apr 03 18:00:06 +0000 2012
            timestamp = LocalDateTime
              .parse(
                data(2),
                DateTimeFormatter.ofPattern("EEE MMM dd kk:mm:ss Z yyyy")
              ),
            offset = data(3).toInt
          )

          db.put((checkin.userId, checkin.timestamp), checkin)
        }
    }.get

    val iterator =
      db.findInRange(
        ("50756", LocalDateTime.of(2012, 4, 4, 0, 0)),
        ("50756", LocalDateTime.of(2012, 4, 5, 0, 0))
      )
        .filter(_.userId == "50756")
    iterator.foreach(println)
  }

  def loadVenues(): Unit = {
    val filename =
      "data/threetriangle/venues.txt"

    val db = new MapKeyValueStore[String, Venue]

    Using(Source.fromFile(filename)) { file =>
      file
        .getLines()
        .take(1000)
        .foreach { line =>
          val data = line.trim.split("\t")
          val venue = Venue(
            venueId = data(0),
            latitude = data(1).toDouble,
            longitude = data(2).toDouble,
            category = data(3),
            country = data(4)
          )

          db.put(venue.venueId, venue)
        }
    }.get

    println(db.get("3fd66200f964a52001ed1ee3"))
  }
}

class MapKeyValueStore[K, V] extends KeyValueStore[K, V] {
  val data: scala.collection.mutable.Map[K, V] =
    scala.collection.mutable.Map.empty

  override def put(key: K, value: V): Unit = data(key) = value

  override def get(key: K): V = data(key)

  override def delete(key: K): Unit = data.remove(key)
}

class TreeKeyValueStore[K: Ordering, V] extends KeyValueStore[K, V] {
  val data: scala.collection.mutable.TreeMap[K, V] =
    scala.collection.mutable.TreeMap.empty

  override def put(key: K, value: V): Unit = data(key) = value

  override def get(key: K): V = data(key)

  override def delete(key: K): Unit = data.remove(key)

  def findAllAfter(key: K): Iterator[V] = data.valuesIteratorFrom(key)

  def findInRange(key1: K, key2: K): Iterator[V] =
    data.range(key1, key2).iterator.map(_._2)
}
