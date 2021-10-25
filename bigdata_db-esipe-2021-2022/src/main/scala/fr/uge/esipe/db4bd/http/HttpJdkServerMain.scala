package fr.uge.esipe.db4bd.http

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{Executors, TimeUnit}

/**
 * Example of HTTP server application, using the JDK API.
 */
object HttpJdkServerMain {
  import scala.jdk.CollectionConverters._

  // PARAMETERS THAT YOU CAN CHANGE

  // Listening port
  val port: Int = 18080

  // Set of tokens considered as valid
  val validTokens: Set[String] =
    Set(
      "b94d27b9934d3e08"
    )

  val path: String = "/api/v1/alert"

  val PostMethod: String          = "POST"
  val AuthorizationHeader: String = "Authorization"

  def main(args: Array[String]): Unit = {
    val server = HttpServer.create(new InetSocketAddress(18080), 0)

    server.createContext(
      path,
      { (exchange: HttpExchange) =>
        try {
          // 0. check the HTTP method
          if (exchange.getRequestMethod != PostMethod) {
            sendMethodNotAllowed(exchange)
          } else {
            // find the Authorization header
            val authorization: Option[(String, util.List[String])] =
              exchange.getRequestHeaders.asScala
                .find(header => {
                  header._1 == AuthorizationHeader
                })

            // 1. check if the Authorization header is present
            if (authorization.isEmpty) {
              sendUnauthorized(exchange, "No authorization token found.")
            } else {
              val (_, values: util.List[String]) = authorization.get

              // 2. that it has associated values
              if (values.isEmpty) {
                sendUnauthorized(exchange, "No authorization token found.")
              }
              // 3. check if the value/token is authorized
              else if (!isAuthorized(values.get(0))) {
                sendUnauthorized(exchange, "Unauthorized.")
              }
              // 4. if authorized, then process the request
              else {
                processRequest(exchange)
              }
            }
          }
        } finally {
          exchange.close()
        }
      }
    )

    sys.addShutdownHook(server.stop(0))
    val threadPool = Executors.newFixedThreadPool(4)
    server.setExecutor(threadPool)

    server.start()
    println(s"Server started")
    println(s"listening on http://localhost:$port")
    threadPool.awaitTermination(9999, TimeUnit.DAYS)
  }

  def processRequest(exchange: HttpExchange): Unit = {
    println("get alert message")

    val data    = exchange.getRequestBody.readAllBytes()
    val content = new String(data, StandardCharsets.UTF_8)

    try {
      processTrackingMessage(content)

      // 200: Ok
      exchange.sendResponseHeaders(200, 0)
      exchange.getResponseBody.write(Array.empty[Byte])
    } catch {
      case _: Exception =>
        // 500: Internal server error
        exchange.sendResponseHeaders(500, 0)
    }
  }

  // THIS AN EXAMPLE OF PROCESSING FUNCTION...
  def processTrackingMessage(content: String): Unit = {
    println(s"\tMessage content: $content")
  }

  // THIS AN EXAMPLE OF AUTHORIZATION PERDICATE...
  def isAuthorized(bearer: String): Boolean = {
    val fields = bearer.split("\\s+", 2)

    fields.size == 2 & fields(0) == "Bearer" && validTokens.contains(fields(1))
  }

  /**
   * Send 401 (Unauthorized) response to the client.
   *
   * @param exchange server-client exchange interface
   * @param content error message to transmit to the client
   */
  def sendUnauthorized(exchange: HttpExchange, content: String): Unit = {
    println(s"Authorization error: $content")
    val data = content.getBytes(StandardCharsets.UTF_8)
    // 401: Unauthorized
    exchange.sendResponseHeaders(401, data.length)
    exchange.getResponseBody.write(data)
  }

  /**
   * Send 405 (Method not allowed) response to the client.
   *
   * @param exchange server-client exchange interface
   */
  def sendMethodNotAllowed(exchange: HttpExchange): Unit = {
    println(s"Method not allowed: ${exchange.getRequestMethod}")
    // 405: Method not allowed
    exchange.sendResponseHeaders(405, 0)
  }
}
