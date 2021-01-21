
import SearchTweets.tweetAnalysis
import SentimentAnalysis.sentimentAnalysis
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Paths
import java.nio.file.Files
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object StreamDemo {
   def main(args: Array[String]): Unit = {
         Future {
             tweetStreamToDir()
         }

         val spark = SparkSession.builder()
             .appName("TwitterDemo")
             .master("local[4]")
             .getOrCreate()
         import spark.implicits._
         spark.sparkContext.setLogLevel("WARN")
//         println(System.getProperty("user.dir"))
//         Thread.sleep(20000)
//         val staticDF = spark.read.json("tweetstream.tmp")
//        val schema = new StructType()
//          .add("id", IntegerType, true)
//          .add("text", StringType, true)
         val staticDF = spark.read.json("sampleResponse")
         val streamDF = spark.readStream.schema(staticDF.schema).json("twitterstream")

//         val textQuery = streamDF.select($"data.text").writeStream.outputMode("append").format("console").start()
//          val tweetDF = streamDF.select($"data.text")
//          var tweetDF = streamDF.select(explode($"data").as("tweetList")).select("tweetList.*")
//            sentimentAnalysis(textQuery)


         textQuery.awaitTermination(180000)
     }

    
    def tweetStreamToDir() {
        val httpClient = HttpClients.custom.setDefaultRequestConfig(
            RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
        ).build
      //this endpoint will work
//        val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
//        val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
      //testing this endpoint
        val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream")
        val httpGet = new HttpGet(uriBuilder.build)
//        val bearerToken = System.getenv("BEARER_TOKEN")
        val bearerToken = "YOUR BEARER TOKEN HERE"
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
        val response = httpClient.execute(httpGet)
        val entity = response.getEntity()
        if (entity != null) {
            val reader = new BufferedReader(new InputStreamReader(entity.getContent()))
            var line = reader.readLine()
            var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile())
            var lineNumber = 1
            var linesPerFile = 1000
            val milliseconds = System.currentTimeMillis()
            while (line != null) {
                if (lineNumber % linesPerFile == 0) {
                    fileWriter.close()
                    Files.move(
                        Paths.get("tweetstream.tmp"),
                        Paths.get(s"twitterstream/tweetstream-${milliseconds}-${lineNumber/linesPerFile}")
                    )
                    fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile())
                }
                fileWriter.println(line)
                line = reader.readLine()
                lineNumber += 1
            }
        }
    }
}