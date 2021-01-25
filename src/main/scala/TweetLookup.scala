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


object TweetsDemo {

  // @throws[IOException]
  // def main(args: Array[String]): Unit = {
  //   var bearerToken: String = System.getenv("BEARER_TOKEN")

  //   if (bearerToken != null) {
  //         Future {
    
  //       // A single tweet id
  //     var response: String = getTweets("1349422509213458432", bearerToken)
  //     println(response)
  //         }
  //   } else {
  //     println("There was a problem getting you bearer token. Please make sure you set the BEARER_TOKEN environment variable")
  //   }
  //   Thread.sleep(3000)
  // }

  @throws[IOException]
  def getTweets(id: String, bearerToken: String): String ={
    var tweetResponse: String = ""
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
            RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
        ).build

    //to lookup a single Tweet
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets?ids=${id}")
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