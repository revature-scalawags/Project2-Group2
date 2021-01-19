import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.NameValuePair
import org.apache.http.client.HttpClient
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.IOException
import java.net.URISyntaxException
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.parsing.json._

object SearchTweets {

  @throws[IOException]
  def main(args: Array[String]): Unit = {

    var bearerToken: String = "YOUR API KEY HERE"

      if (bearerToken != null) {
        val tweets = Future {
          var response: String = getTweets("BurgerKing", bearerToken)
          println(response)
          //deconstruct JSON object to find next_token
          val json: Option[Any] = JSON.parseFull(response)
          val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
          val meta: Map[String, String] = map.get("meta").get.asInstanceOf[Map[String, String]]
          //if there is next_token, call retrievePages function to recursively retrieve all pages
          if (meta.contains("next_token")) {
            val next_token = meta("next_token")
            retrievePages(next_token, bearerToken)
          }
        }
        Await.ready(tweets, Duration.Inf)
      }
      else {
        println("There was a problem getting you bearer token. Please make sure you set the BEARER_TOKEN environment variable")
    }
  }

  //recursively retrieve all pages from API endpoint, printing them to console
  def retrievePages(next_token: String, bearer_token: String): Unit = {
    var response: String = getTweets("BurgerKing", bearer_token, next_token)
    println(response)

    val json: Option[Any] = JSON.parseFull(response)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    val meta: Map[String, String] = map.get("meta").get.asInstanceOf[Map[String, String]]
    if (meta.contains("next_token")) {
      val next_token = meta("next_token")
      retrievePages(next_token, bearer_token)
    }
  }

  @throws[IOException]
  def getTweets(query: String, bearerToken: String): String ={
    var tweetResponse: String = ""
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
            RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
        ).build

    //to search Tweets based on query
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/search/recent?query=from:${query}")

    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", s"Bearer ${bearerToken}")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (entity != null) {
      tweetResponse = EntityUtils.toString(entity, "UTF-8")
    }
    return tweetResponse
  }

  //overloaded getTweets with next_token argument
  @throws[IOException]
  def getTweets(query: String, bearerToken: String, next_token: String): String ={
    var tweetResponse: String = ""
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
      RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
    ).build

    //to search Tweets based on query
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/search/recent?query=from:${query}&next_token=${next_token}")

    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", s"Bearer ${bearerToken}")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (entity != null) {
      tweetResponse = EntityUtils.toString(entity, "UTF-8")
    }
    return tweetResponse
  }
}