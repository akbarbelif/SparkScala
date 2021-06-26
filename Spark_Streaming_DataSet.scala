package Streaming

import common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Spark_Streaming_DataSet {
   val spark = SparkSession
     .builder()
     .master("local")
     .appName("SparkStreamingDataSet")
     .getOrCreate()

   //import spark.implicits._

   def readCars(): Dataset[Car] = {
     // Useful for DF -> DS transformations
     val carEncoder = Encoders.product[Car]

     spark.readStream
       .format("socket")
       .option("host","localhost")
       .option("port","12349")
       .load() // DF with single column "value"
       .select(from_json(col("value"),carsSchema).as("car"))
       .selectExpr("car.*")
       .as[Car](carEncoder) // we pass encoder here
      // we could have also done .as[Car] if we had used spark.implicit._
   }
   def showCarNames() = {
     val carsDS: Dataset[Car] = readCars()

     import spark.implicits._
     //transformation
     val carNamesDF: DataFrame = carsDS.select(col("Name"))

     //Below we get DS from DS as its map
     val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

     carNamesAlt.writeStream
       .format("console")
       .outputMode("append")
       .start()
       .awaitTermination()
   }


   /*Below is finding HorsePower > 140*/
   def ex1() = {
     val carsDS = readCars()
     carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
       .writeStream
       .format("console")
       .outputMode("append")
       .start()
       .awaitTermination()
   }

  /*Average Horse Power*/
  def ex2() = {
    val carsDS = readCars()

    carsDS.select(avg(col("HorsePower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /*Below is count based on origin-CHECK WHY BELOW HAS ERROR*/
  /*  def ex3() = {
    val carsDS = readCars()
    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // option 1
    val carCountByOriginAlt = carsDS.groupByKey(car => car.Origin).count() // option 2 with the Dataset API
    carCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }*/

  def main(args: Array[String]): Unit ={
    showCarNames()
  }
}
