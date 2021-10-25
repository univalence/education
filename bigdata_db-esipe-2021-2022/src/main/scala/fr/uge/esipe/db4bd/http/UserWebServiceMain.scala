package fr.uge.esipe.db4bd.http

import scala.collection.mutable
import spark.{Request, Response}
import spark.Spark._

/**
 * User Web service.
 */
object UserWebServiceMain {

  val servicePort = 18090

  val database = new UserTokenRepository
  locally {
    database.addAll(
      // token, user_id
      ("Xzm1Gumk2su42VOCKdcmv7fhoDYz431kWYwymJqMEnc", "0000"),
      ("rrrFPEa77_EP3SbKDiGWqb_B0Zv4jrHv1lo2FRxYEFE", "0001"),
      ("gfO_Qqk88Y3s6TIaxckzExJutcqSFk10ZD5Mv2Ds3pw", "0002"),
      ("XSQ_YJa7iK6XfzEZrFru4jmiyqQmYJWAi9CxbKNQqhc", "0003"),
      ("Jg9_aJKMDVJsG98rT1zYNy7IHzSfZOFNC0FNCfhkNkM", "0004"),
      ("OdOi6Wmz3sEUeNZey5buYGbr2YkgyBbfnefRMdnGfQw", "0005"),
      ("4mRrbW1yrEsVgOxUgiBMaFf__R9M6t0frnMg__7pysM", "0006"),
      ("E1kBwrJb6zZW9SCChys9LZ32PtGG4yQJxRCfGDiOdxA", "0007"),
      ("UrZ7YCYNo5N1EK1UXH9G-NmRW9J-EILnaUf7MJ-RO9M", "0008"),
      ("TBiwjrEUhsuuAnDf_fnPsIyn7ocbwuRdme-KhrR_YNo", "0009"),
      ("G04eGxwuIzmc2qGNX0XAG2B3hWHuhf5yjVh-xDhsJpg", "0010"),
      ("9VNzoe75_6T7tr-EdEqOb1Xx3QV1SYox2btvZ8qwi7E", "0011"),
    )
  }

  val BearerTokenParameter = "bearer_token"

  def main(args: Array[String]): Unit = {
    port(servicePort)
    get("/user", (request: Request, response: Response) => {
      val token = request.queryParams(BearerTokenParameter)

      if (token == null) {
        sendNotFound(response)
      } else {
        val userId: Option[String] = database.find(token)

        if (userId.isEmpty) {
          sendNotFound(response)
        } else {
          response.`type`("application/json")

          s"""{"user_id": "${userId.get}"}"""
        }
      }
    })
  }

  def sendNotFound(response: Response): String = {
    response.status(404)
    response.`type`("text/plain")
    "404 Not Found"
  }

}

class UserTokenRepository {
  val data: mutable.Map[String, String] = mutable.Map.empty

  def add(token: String, userId: String): Unit = data(token) = userId
  def addAll(tokens: (String, String)*): Unit  = data.addAll(tokens)
  def remove(token: String): Unit              = data.remove(token)
  def find(token: String): Option[String]      = data.get(token)
}
