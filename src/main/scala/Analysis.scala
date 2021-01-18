import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    analysis(spark)
    spark.stop()
  }

  def analysis(spark: SparkSession): Unit = {
    import spark.implicits._
    val test = Seq(
      (8, "negative"),
      (9, "positive"),
      (7, "positive"),
      (2, "negative"),
      (3, "positive"),
      (4, "mixed"),
    ).toDF("number", "sentiment")
    //show counts per sentiment
    test.groupBy("sentiment").count().show()
    //show percentage of counts per sentiment
    test.groupBy("sentiment").count().withColumn("sentiment_percentage", (col("count") / sum("count").over()) * 100).show()
    //show percentage of counts per sentiment pretty format but there is bug- showing too many decimal places
    test.groupBy("sentiment").count().withColumn("sentiment_percentage", format_number(col("count") / sum("count").over(), 4) * 100).show()
  }
}
