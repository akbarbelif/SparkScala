package Streaming

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka_DStream {
  val spark = SparkSession.builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  /*Read from kafka using DStream*/
  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      /*
       Distributes the partitions evenly across the Spark cluster.
       Alternatives:
       - PreferBrokers if the brokers and executors are in the same cluster
       - PreferFixed
      */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
      /*
        Alternative
        - SubscribePattern allows subscribing to topics matching a pattern
        - Assign - advanced; allows specifying offsets and partitions per topic
       */
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost",12345)

    //transform data
    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD{rdd  =>
      rdd.foreachPartition{ partition =>
        // inside this lambda, the code is run by a single executor

        val kafkaHashMap = new util.HashMap[String,Object]()
        // below we put config data from kafkaParams into the kafkaHashMap
        kafkaParams.foreach{ pair =>
           kafkaHashMap.put(pair._1,pair._2)
        }
        // producer can insert  records into the Kafka topics
        // available on this executor
        // here kafkaHashMap is the config passed - the producer produced for each partition
        // because we cant produce once for driver then serialize hence each is for each partition i.e each executor
        val producer = new KafkaProducer[String,String](kafkaHashMap)

        partition.foreach{ value =>
             val message = new ProducerRecord[String,String](kafkaTopic,null,value)  // null is the key, value is value passed in 12345 terminal
             // feed the message into the Kafka topic
             producer.send(message)
        }
        producer.close()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // readFromKafka()
       writeToKafka()
  }
}
