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

import java.io.IOException;
import java.net.URISyntaxException;
import scala.collection.mutable.ArrayBuffer


object TweetsDemo {

  // To set your enviornment variables in your terminal run the following line:
  // export 'BEARER_TOKEN'='<your_bearer_token>'

  @throws[IOException]
  def main(args: Array[String]): Unit = {
    //var bearerToken: String = System.getenv("BEARER_TOKEN")
    var bearerToken: String = ""
    if (bearerToken != null) {
      //Replace comma separated ids with Tweets Ids of your choice
      var response: String = getTweets("1349422509213458432", bearerToken)
      println(response)
    } else {
      println("There was a problem getting you bearer token. Please make sure you set the BEARER_TOKEN environment variable")
    }
  }

  /*
   * This method calls the v2 Tweets endpoint with ids as query parameter
   * */
  @throws[IOException]
  def getTweets(ids: String, bearerToken: String): String ={
    var tweetResponse: String = null;

    val httpClient = HttpClients.custom.setDefaultRequestConfig(
            RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
        ).build

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/users/by/username/:Burgerking")
    val httpGet = new HttpGet(uriBuilder.build)
    val bearerToken = System.getenv("BEARER_TOKEN")
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

    var queryParameters: java.util.List[NameValuePair] = new java.util.ArrayList[NameValuePair]()
    
    queryParameters.add(new BasicNameValuePair("ids", ids))
    queryParameters.add(new BasicNameValuePair("tweet.fields", "created_at"))
    uriBuilder.addParameters(queryParameters)
    // uriBuilder.addParameters(new BasicNameValuePair("ids", ids))
    // uriBuilder.addParameters(new BasicNameValuePair("tweet.fields", "created_at"))
    
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
    httpGet.setHeader("Content-Type", "application/json");

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      tweetResponse = EntityUtils.toString(entity, "UTF-8")
    }
    return tweetResponse
  }
}