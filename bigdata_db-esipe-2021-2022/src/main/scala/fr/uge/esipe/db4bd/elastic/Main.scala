package fr.uge.esipe.db4bd.elastic

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import scala.io.Source
import scala.util.Using

object Main {
  def main(args: Array[String]): Unit = {
    val client = new RestHighLevelClient(
      RestClient.builder(
        new HttpHost("localhost", 9200)
      )
    )

    try {
      Using(Source.fromFile("data/tweets.json")) { file =>
        for (line <- file.getLines()) {
          try {
            val id = line.split(",", 2).head.split(":")(1)

            val indexRequest = new IndexRequest("tweets")
            indexRequest.id(id).source(line.trim, XContentType.JSON)
            val response = client.index(indexRequest, RequestOptions.DEFAULT)
            println(s"$id: ${response.getResult.name()}")
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      }.get
    } finally {
      client.close()
    }

  }
}
