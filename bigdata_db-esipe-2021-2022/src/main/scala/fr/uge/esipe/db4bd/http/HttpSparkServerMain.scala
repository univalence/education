package fr.uge.esipe.db4bd.http

import spark.Spark._
import spark.{Request, Response}

object HttpSparkServerMain {

  // PARAMETERS THAT YOU CAN CHANGE

  // Listening port
  val serverPort: Int = 18080

  // Set of tokens considered as valid
  val validTokens: Set[String] =
    Set(
      "b94d27b9934d3e08"
    )

  val AuthorizationHeader: String = "Authorization"

  def main(args: Array[String]): Unit = {
    port(serverPort)
    post(
      "/api/v1/alert",
      (request: Request, response: Response) => {
        val bearer = request.headers(AuthorizationHeader)
        if (!isAuthorized(bearer)) {
          // 401: Unauthorized
          response.status(401)

          ""
        } else {
          processRequest(request, response)
        }
      }
    )
  }

  def processRequest(request: Request, response: Response): String = {
    println("get alert message")

    val content = request.body()

    try {
      processTrackingMessage(content)

      // 200: Ok
      response.status(200)
      ""
    } catch {
      case e: Exception =>
        // 500: Internal server error
        throw e
    }
  }

  // THIS AN EXAMPLE OF PROCESSING FUNCTION...
  def processTrackingMessage(content: String): Unit = {
    println(s"\tMessage content: $content")
  }

  // THIS AN EXAMPLE OF AUTHORIZATION PERDICATE...
  def isAuthorized(bearer: String): Boolean = {
    if (bearer == null) {
      false
    } else {
      val fields = bearer.split("\\s+", 2)

      fields.size == 2 & fields(0) == "Bearer" && validTokens.contains(fields(1))
    }
  }

}
