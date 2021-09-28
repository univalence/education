package fr.uge.esipe.db4bd

import fr.uge.esipe.db4bd.model.Venue
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.rocksdb.RocksDB
import scala.io.Source
import scala.util.Using

object VenuesMain {
  def main(args: Array[String]): Unit = {
    val filename = "data/threetriangle/venues.txt"

    RocksDB.loadLibrary()

    Using(RocksDB.open("data/db")) { db =>
//      val start = System.nanoTime()
//      Using(Source.fromFile(filename)) { file =>
//        file
//          .getLines()
////          .take(1000)
//          .foreach { line =>
//            val data = line.trim.split("\t")
//            val venue = Venue(
//              venueId = data(0),
//              latitude = data(1).toDouble,
//              longitude = data(2).toDouble,
//              category = data(3),
//              country = data(4)
//            )
//
//            db.put(venue.venueId.getBytes(StandardCharsets.UTF_8), venue.serialize)
//          }
//      }.get
//      val end = System.nanoTime()
//      println((end - start) / 1e-9)
      println("get data")
      val raw: Array[Byte] = db.get("3fd66200f964a52010e51ee3".getBytes(StandardCharsets.UTF_8))
      val result: Venue = Venue.deserialize(ByteBuffer.wrap(raw))
      println(result)
    }
  }
}
