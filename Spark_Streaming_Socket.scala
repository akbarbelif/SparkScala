package Streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
/* First execute below in terminal and then start consuming in socket
nc -lk 12345
*/

object Spark_Streaming_Socket {
  val spark = SparkSession.builder()
    .appName("First Streaming")
    .master("local[2]")
    .getOrCreate()
   //Reading Dataframe
  def readFromSocket() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    //transformation
    val shortLines: DataFrame = lines.filter(length(col("value"))<=5)

    println(shortLines.isStreaming)

    //Consuming Dataframe
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def readFromFile() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header","false")
      .option("dateFormat","MMM d yyyy")
      .schema(stocksSchema)   // need to pass schema while reading from streaming datasource
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2.seconds"))  // every 2 seconds run the query
     //Trigger.Once()  // single batch and then terminate
     //Trigger.Continuous(2.seconds) // experimental, every 2 second create a batch with whatever you have
  }

  def main(args: Array[String]): Unit = {
     // readFromSocket()
        readFromFile()
  }
}
