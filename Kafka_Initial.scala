package Streaming

import common.carsSchema
import org.apache.spark.sql.functions.{to_json, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Kafka_Initial {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()


  /*Start bin/kafka-console-producer.sh --broker-list localhost:9092 --topic rockthejvm
  * and put some data in command line pf docker machine*/
  def readFromKafka() = {

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","rockthejvm")
      .load()

    /*Initially key,value,topic,partition,offset,timestamp,timestampType columns there without the select below*/
    kafkaDF
      .select(col("topic"),expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("/home/bose/Downloads/SparkLearning/src/main/scala/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","rockthejvm")
      .option("checkpointLocation", "checkpoints")  // without checkpoint the writing to Kafka fails
       .start()
      .awaitTermination()
  }

  /*writing json to kafka*/
  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("/home/bose/Downloads/SparkLearning/src/main/scala/data/cars")

    val carsJsonKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
    )

    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }
  def main(args:Array[String]): Unit ={
   // readFromKafka()   //For Reading from kafka
   // writeToKafka()    //For Writing to Kafka
    writeCarsToKafka()  //Writing json to kafka
  }
}
