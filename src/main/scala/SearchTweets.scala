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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.IOException
import java.net.URISyntaxException
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.parsing.json._
import scala.util.Try
import org.apache.spark.sql.functions.explode

object SearchTweets {

  @throws[IOException]
  def main(args: Array[String]): Unit = {
    var bearerToken: String = System.getenv("BEARER_TOKEN")

      if (bearerToken != null) {
        // setup spark session
        val spark = SparkSession.builder()
          .appName("searchTweets")
          .master("local[4]")
          .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("WARN")

        //create a dataframe out of our response json so we can start working with it
        val responseDF = spark.read.json(Seq(getTweets("BurgerKing", bearerToken)).toDS)

        //deconstruct response DF to start building our tweetDF find next_token
        var tweetDF = responseDF.select(explode($"data").as("tweetList")).select("tweetList.*")
        val metaDF = responseDF.select($"meta.*")

        //if there is next_token, call retrievePages function to recursively retrieve all pages
        if (Try(metaDF("next_token")).isSuccess) {
          val next_token = metaDF.select("next_token").first().getString(0)
          tweetDF = retrievePages(next_token, bearerToken, tweetDF, spark)
        }

        // At this point the tweetDF contains our desired tweets in the text column
        tweetDF.show()
        tweetDF.printSchema()

        spark.stop()
      }
      else {
        println("There was a problem getting you bearer token. Please make sure you set the BEARER_TOKEN environment variable")
    }
  }

  //recursively retrieve all pages from API endpoint, printing them to console
  def retrievePages(next_token: String, bearer_token: String, buildingDF: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val responseDF = spark.read.json(Seq(getTweets("BurgerKing", bearer_token, next_token)).toDS())
    var tweetDF = responseDF.select(explode($"data").as("tweetList")).select("tweetList.*")

    // append our new DF to the old DF so we can pass it to the next reccursive call
    tweetDF = buildingDF.union(tweetDF)

    //check if we can continue down the next_token reccursive call
    val meta = responseDF.select($"meta.*")
    if (Try(meta("next_token")).isSuccess) {
      val next_token = meta.select("next_token").first().getString(0)
      return retrievePages(next_token, bearer_token, tweetDF, spark)
    }

    //else we are finished
    else
      return tweetDF
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