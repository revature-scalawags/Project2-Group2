import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import java.io.IOException;
import java.net.URISyntaxException;
import scala.collection.mutable.ArrayBuffer


object IdLookup {

  @throws[IOException]
  def main(args: Array[String]): Unit = {
    var bearerToken: String = ""

    if (bearerToken != null) {
          Future {
    
        //ID to look up goes here
      var response: String = getTweets("BurgerKing", bearerToken)
      println(response)
          }
    } else {
      println("There was a problem getting you bearer token. Please make sure you set the BEARER_TOKEN environment variable")
    }
    Thread.sleep(3000)
  }

  @throws[IOException]
  def getTweets(userid: String, bearerToken: String): String ={
    var tweetResponse: String = ""
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
            RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
        ).build

    
    //to lookup a user's detailed info based on id name
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/users/by/username/${userid}") 

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