package fr.uge.esipe.db4bd.http

import java.time.Instant
import java.util.concurrent.{Executors, TimeUnit}
import okhttp3._

/**
 * This application sends tracking messages by using HTTP protocol to a
 * Web server.
 *
 * This application also shows you a way to use the OkHttp library
 * (https://square.github.io/okhttp/).
 */
object TrackingSenderApp {

  import scala.jdk.CollectionConverters._

  // PARAMETERS THAT YOU CAN CHANGE

  // period in milliseconds. Change it for the stream to go faster.
  val period: Long = 60 * 1000
  // token that represents a session on a phone.
  val token: String = "b94d27b9934d3e08"

  // Web service host
  val host: String = "localhost"
  // Web service listening port
  val port: Int = 18080
  // Web service path
  val path: String = "/api/v1/alert"

  // Phone tracking scenario (the program cycles over this list).
  // note: the timestamp depends on the defined period.
  val scenario: List[((Double, Double), String)] =
    List(
      // (lat,long), reason
      ((48.847700, 2.333443), "TRACKING"),
      ((48.847730, 2.333443), "TRACKING"),
      ((48.847760, 2.333443), "TRACKING"),
      ((48.847790, 2.333443), "TRACKING"),
      ((48.847820, 2.333443), "ZONEOUT"),
      ((48.847850, 2.333443), "ZONEOUT"),
      ((48.847820, 2.333443), "ZONEOUT"),
      ((48.847790, 2.333443), "TRACKING"),
      ((48.847760, 2.333443), "TRACKING"),
      ((48.847730, 2.333443), "TRACKING")
    )

  def main(args: Array[String]): Unit = {
    val url: String          = s"http://$host:$port$path"
    val client: OkHttpClient = new OkHttpClient()
    val scheduler            = Executors.newSingleThreadScheduledExecutor()

    // shutdown the scheduler, whatever way this application is stopped
    sys.addShutdownHook(scheduler.shutdownNow())

    // Iterator that repeats over the scenario
    val iterator: Iterator[((Double, Double), String)] =
      Iterator
        .from(0)
        .map(i => scenario(i % scenario.size))

    // Schedule the scenario
    scheduler.scheduleAtFixedRate(
      () => {
        val (coordinates, reason) = iterator.next()

        sendTracking(url, coordinates, Instant.now(), reason, token, client)
      },
      0L,
      period,
      TimeUnit.MILLISECONDS
    )

    scheduler.awaitTermination(1000, TimeUnit.DAYS)
  }

  /**
   * Send a tracking HTTP message to a Web server.
   *
   * @param url service URL
   * @param coordinates pair of latitude and longitude
   * @param timestamp timestamp for the tracking message
   * @param reason reason of the tracking message
   * @param token token associated to the user
   * @param client client connection to the Web server
   */
  def sendTracking(
      url: String,
      coordinates: (Double, Double),
      timestamp: Instant,
      reason: String,
      token: String,
      client: OkHttpClient
  ): Unit = {
    val content =
      s"""{"coordinates": [${coordinates._1}, ${coordinates._2}], "timestamp": ${timestamp.toEpochMilli}, "reason": $reason}"""

    println(s"""Sending by $token: $content""")

    val body: RequestBody =
      RequestBody.create(
        content,
        MediaType.get("application/json")
      )

    val request: Request =
      new Request.Builder()
        .url(url)
        .addHeader("Authorization", "Bearer " + token)
        .post(body)
        .build()

    val response: Response = client.newCall(request).execute()
    try {
      val code    = response.code()
      val message = response.message()
      val headers = response.headers().asScala
      println(s"Response: $code $message")
      println("\tHeaders:")
      headers
        .map(p => p.component1() -> p.component2())
        .foreach(p => println(s"\t\t$p"))
    } finally {
      response.close()
    }
  }
}
