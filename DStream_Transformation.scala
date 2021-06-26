package Streaming

/* Need to execute below in command line
cat people-1m.txt | nc -lk 9998*/
import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

/*
cd udemy-spark-streaming-master1/src/main/resources/data/people-1m
cat people-1m.txt | nc -lk 9998
*/
object DStream_Transformation {
   val spark = SparkSession.builder()
     .appName("DStream Transformation")
     .master("local[2]")
     .getOrCreate()

   val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

   /*map on DStream*/
   def peopleAges(): DStream[(String,Int)] = readPeople().map { person =>
       val age = Period.between(person.birthDate.toLocalDate,LocalDate.now()).getYears
       (s"${person.firstName} ${person.lastName}", age)
   }

   /*flatMap on DStream*/
   def peopleSmallNames(): DStream[String] = readPeople().flatMap { person =>
       List(person.firstName , person.middleName)
   }

  def highIncomePeople() = readPeople().filter(_.salary > 80000)

  def countPeople(): DStream[Long] = readPeople().count()

  //count by value, PER BATCH not on full
  def countNames(): DStream[(String,Long)] = readPeople().map(_.firstName).countByValue()

  /*reduce by key
    - works on DStream of tuple
    - works PER BATCH
      In this example key is String and Value is Int
   */
  def countNamesReduce: DStream[(String,Int)] = readPeople()
    .map(_.firstName)
    .map(name => (name,1))
    .reduceByKey((a,b) => a + b)

  /*Below we save stream data into json files based on batch -somehow not working*/
  import spark.implicits._
  def saveToJson() = readPeople().foreachRDD { rdd =>
    val ds = spark.createDataset(rdd)
    val f = new File("/home/bose/Downloads/SparkLearning/src/main/scala/outputFiles/people")
    val nFiles = f.listFiles().length
    val path = s"/home/bose/Downloads/SparkLearning/src/main/scala/outputFiles/people$nFiles.json"

    ds.write.json(path)
  }


   def readPeople() = ssc.socketTextStream("localhost",9997).map { line =>
     val tokens = line.split(":")
     Person(
           tokens(0).toInt,   // Id
           tokens(1),         // first name
           tokens(2),         // middle name
           tokens(3),         // last name
           tokens(4),          // gender
           Date.valueOf(tokens(5)),  // birth
           tokens(6),            // ssn/uuid
           tokens(7).toInt      //  salary
      )
   }
   def main(args:Array[String]): Unit = {
    // val stream = readPeople()  // reading a datastream
    // val stream = peopleSmallNames()  // reading names using flatMap
    // val stream = highIncomePeople() // highest income
    //   val stream = countPeople()  // count of people
    saveToJson() //saving in json files
   //  stream.print() only for commented calls as there we print not this one
     ssc.start()
     ssc.awaitTermination()

     /*done till 20:33*/
   }
}
