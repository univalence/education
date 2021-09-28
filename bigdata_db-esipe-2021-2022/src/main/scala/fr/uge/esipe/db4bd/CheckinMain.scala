package fr.uge.esipe.db4bd

import fr.uge.esipe.db4bd.model.Checkin
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.rocksdb.{ReadOptions, RocksDB, Slice}
import scala.io.Source
import scala.util.Using

object CheckinMain {
  def main(args: Array[String]): Unit = {
    RocksDB.loadLibrary()

    val filename =
      "/Users/fsarradin/src/test/2021_06/mini_kafka/data/threetriangle/dataset_TIST2015_Checkins.txt"
    Using(RocksDB.open("data/checkin")) { db =>
//      Using(Source.fromFile(filename)) { file =>
//        file
//          .getLines()
////          .take(100000)
//          .foreach { line =>
//            val data = line.trim.split("\t")
//            val checkin = Checkin(
//              userId = data(0),
//              venueId = data(1),
//              // Tue Apr 03 18:00:06 +0000 2012
//              timestamp = LocalDateTime
//                .parse(
//                  data(2),
//                  DateTimeFormatter.ofPattern("EEE MMM dd kk:mm:ss Z yyyy")
//                ),
//              offset = data(3).toInt
//            )
//
//            val key = s"${checkin.userId}#${checkin.timestamp}"
//
//            db.put(key.getBytes(StandardCharsets.UTF_8), checkin.serialize)
//          }
//      }

      val start = LocalDateTime
        .of(2012, 4, 4, 0, 0)
        .atOffset(ZoneOffset.UTC)
        .toInstant
      val end = LocalDateTime
        .of(2012, 4, 5, 0, 0)
        .atOffset(ZoneOffset.UTC)
        .toInstant

      val key1 = s"50756#$start"
      val key2 = s"50756#$end"
      val readOption = new ReadOptions()
        .setIterateLowerBound(new Slice(key1))
        .setIterateUpperBound(new Slice(key2))

      println(s"from: $key1 to: $key2")

      val iterator = db.newIterator(readOption)
      iterator.seekToFirst()
      while (iterator.isValid) {
        val key = new String(iterator.key(), StandardCharsets.UTF_8)
        val value = Checkin.deserialize(ByteBuffer.wrap(iterator.value()))

        println(s"$key -> $value")
        iterator.next()
      }
    }
  }
}
