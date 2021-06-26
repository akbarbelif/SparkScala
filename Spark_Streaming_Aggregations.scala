package Streaming
/* execute in terminal
nc -lk 12345
*/
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Spark_Streaming_Aggregations {

  val spark = SparkSession
    .builder()
    .appName("Spark_Streaming")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported

    lineCount.writeStream
      .format("console")
      .outputMode("Complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregation(aggFunction: Column => Column):Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    //aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // counting occurences of the "name" value
    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name"))
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    //streamingCount()
    //numericalAggregation(stddev)
      groupNames()
  }
}
