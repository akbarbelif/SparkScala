package Streaming

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DStream {

   val spark = SparkSession.builder()
     .master("local2]")
     .appName("DStream")
     .getOrCreate()

  /*
  Spark Streaming Context = entry point to the DStream API
  - needs the spark context
  - a duration = batch interval
  */
   val ssc = new StreamingContext(spark.sparkContext,Seconds(1)) // here batch interval is 1sec
   /*
   - define input sources by creating DStreams
   - define transformations on DStreams
   - start ALL computation with ssc.start
     - no more computation can be added after ssc.start
   - await termination ,or stop the computation
     - you cannot restart the ssc
   */
   def readSocket() = {
     val socketStream: DStream[String] = ssc.socketTextStream("localhost",12345)

     //transformations = lazy
     val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

     //action
     //wordsStream.print()
     // if we want to write to file from socket
     // each folder = RDD = batch, each file = a partition of the RDD
     wordsStream.saveAsTextFiles("/home/bose/Downloads/SparkLearning/src/main/scala/outputFiles/")


     ssc.start()
     ssc.awaitTermination()
   }

   /*createNewFile - creates extra file*/
   def createNewFile() = {
     new Thread(() => {
       Thread.sleep(5000)

       val path = "src/main/resources/data/stocks"
       val dir = new File(path) // directory where I will store a new file
       val nFiles = dir.listFiles().length
       val newFile = new File(s"$path/newStocks$nFiles.csv")
       newFile.createNewFile()

       val writer = new FileWriter(newFile)
       writer.write(
         """
           |AAPL,Sep 1 2000,12.88
           |AAPL,Oct 1 2000,9.78
           |AAPL,Nov 1 2000,8.25
           |AAPL,Dec 1 2000,7.44
           |AAPL,Jan 1 2001,10.81
           |AAPL,Feb 1 2001,9.12
        """.stripMargin.trim)

       writer.close()
     }).start()
   }


   /*DStream file read detects new file and its contents*/
   def readFromFile() = {
     createNewFile()  //operates on another thread i.e asynchronous
     // defined DStream
     val stocksFilePath = "/home/bose/Downloads/SparkLearning/src/main/resources/data/stocks"
     /*
     ssc.textFileStream monitors a directory for NEW FILE.
     */

     val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

     //transformation
     val dateFormat = new SimpleDateFormat("MMM d yyyy")

     val stocksStream: DStream[Stock] = textStream.map {line =>
       val tokens = line.split(",")
       val company = tokens(0)
       val date = new Date(dateFormat.parse(tokens(1)).getTime)
       val price = tokens(2).toDouble

       Stock(company,date,price)
     }

      // action
       stocksStream.print()

     //start the computation
     ssc.start()
     ssc.awaitTermination()
   }

   def main(args:Array[String]): Unit ={
      readSocket()   //Reading using socket and writing to files
     // readFromFile()  //reading from files detecting new files thats created
   }
}
