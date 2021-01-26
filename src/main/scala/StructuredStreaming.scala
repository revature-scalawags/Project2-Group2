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
import org.apache.spark.sql.functions.split


object StructuredStreaming {
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
    val staticDF = spark.read.json("sampleResponse")
    val streamDF = spark.readStream.schema(staticDF.schema).json("twitterstream")
    val id_text = streamDF.select($"data.*")
    val toExclude = Seq("A","The", "But", "Or", "RT", "And", "If", "De", "To", "Que", "Of", "Is", "I", "In", "You", "For", "Me", "La", "On", "No", "That", "En", "This", "I'm", "El", "THE", "He")
    val words = id_text.select(explode(split($"text", " "))).as("text").where(!($"text.col" .isin(toExclude:_*))).filter($"text.col" rlike "[A-Z].+")
    val grouped = words.groupBy($"text.col").count().sort($"count".desc)
    val streamQuery = grouped.writeStream.queryName("counts").outputMode("complete").format("memory").start()
    while(streamQuery.isActive) {
      Thread.sleep(10000)
      spark.sql("select * from counts").show(200, false)
    }

    streamQuery.awaitTermination(200000)
  }


  def tweetStreamToDir() {
      val httpClient = HttpClients.custom.setDefaultRequestConfig(
          RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
      ).build
      val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
      val httpGet = new HttpGet(uriBuilder.build)
      val bearerToken = "AAAAAAAAAAAAAAAAAAAAAKUnLwEAAAAApWuEw9K8SVH7%2BqNNxVtg36Ryr5o%3DP3sGYZngNVtApjFYqjMC5r3lTg8cF4gGIdCtcjUJAjnd23VpEk"
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