package Streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
Step1: Start in command line nc -lk 12346
Copy paste this line in terminal to create streamsDF
{"id":3,"name":"Metallica","hometown":"Los Angeles","year":1981}
*/

object Spark_Stream_Join {
   val spark = SparkSession.builder()
     .appName("Streaming Join")
     .master("local")
     .getOrCreate()

   val guitarPlayers = spark.read
     .option("inferSchema",true)
     .json("/home/bose/Downloads/SparkLearning/src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema",true)
    .json("/home/bose/Downloads/SparkLearning/src/main/resources/data/guitars")

  val bands = spark.read
    .option("inferSchema",true)
    .json("/home/bose/Downloads/SparkLearning/src/main/resources/data/bands")

  //joining static DF
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristBand = guitarPlayers.join(bands,joinCondition,"inner")

  val bandsSchema = bands.schema

  /*
  restricted join:
  - stream joining with static : RIGHT OUTER join/full outer join/right semi not permitted
  - static joining with streaming : LEFT OUTER join/full outer join/left semi not permitted
  */
  def joinStreamWithStatic() = {
      val streamBandsDF = spark.readStream
        .format("socket")
        .option("host","localhost")
        .option("port",12346)
        .load() // a DF with a single column "value" of type String
        .select(from_json(col("value"),bandsSchema).as("band"))
        .selectExpr("band.id as id","band.name as name","band.hometown as hometown","band.year as year")

      val streamBandsGuitaristsDF = streamBandsDF.join(guitarPlayers,guitarPlayers.col("band") === streamBandsDF.col("id"),"inner")

      streamBandsGuitaristsDF.writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()
  }

  // since Spark 2.3 we have stream against stream joining
  def joinStreamWithStream() = {
    /*
    Full Outer Join not supported for stream to stream join
    left/right outer join supported with watermarks only
    inner join supported for stream with stream join
    */
    val streamBandsDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12347)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"),bandsSchema).as("band"))
      .selectExpr("band.id as id","band.name as name","band.hometown as hometown","band.year as year")

    val streamGuitaristDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12348)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"),guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id","guitarPlayer.name as name","guitarPlayer.guitars as guitars","guitarPlayer.band as band")

    // join stream with stream
    val streamedJoin = streamBandsDF.join(streamGuitaristDF,streamGuitaristDF.col("band") === streamBandsDF.col("id"))

    streamedJoin.writeStream
      .format("console")
      .outputMode("append")  // only append supported between stream against stream join
      .start()
      .awaitTermination()
  }

  def main(args:Array[String]): Unit ={
    joinStreamWithStatic()
  }
}
