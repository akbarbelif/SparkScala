package Streaming

import common._
import org.apache.spark.sql.{Dataset, SparkSession}

object Kafka_Stream_Integrating_JDBC {

     val spark = SparkSession.builder()
       .appName("Integrating JDBC")
       .master("local[2]")
       .getOrCreate()

     val driver = "org.postgresql.Driver"
     val url = "jdbc:postgresql://localhost:5432/rtjvm"
     val user = "docker"
     val password = "docker"

     import spark.implicits._

     def writeStreamToPostgres() = {
       val carsDF = spark.readStream
         .schema(carsSchema)
         .json("/home/bose/Downloads/SparkLearning/src/main/scala/data/cars")

       val carsDS = carsDF.as[Car]

       carsDS.writeStream
         .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
           // each executor can control the batch
           // batch is a STATIC Dataset/DataFrame

           batch.write
             .format("jdbc")
             .option("driver", driver)
             .option("url", url)
             .option("user", user)
             .option("password", password)
             .option("dbtable", "public.cars")
             .save()
         }
         .start()
         .awaitTermination()
     }
  def main(args:Array[String]): Unit ={
    writeStreamToPostgres()
   }
}
