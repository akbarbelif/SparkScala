package Others

import org.apache.commons.net.io.Util
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util
/*
    Suppose you have elements from 1 to 100 distributed among 10 partitions i.e. 10
    elements/partition. map() transformation will call func 100 times to process
    these 100 elements but in case of mapPartitions(), func will be called
    once/partition i.e. 10 times.
    Secondly, mapPartitions() holds the data in-memory i.e. it will store the result
    in memory until all the elements of the partition has been processed.
    mapPartitions() will return the result only after it finishes processing of whole
    partition.
    mapPartitions() requires an iterator input unlike map() transformation.
    What is an Iterator? An iterator is a way to access collection of elements
    one-by-one, its similar to collection of elements like List(), Array() etc in
    few ways but the difference is that iterator doesn't load the whole collection
    of elements in memory all together. Instead iterator loads elements one after
    another. In Scala you access these elements with hasNext and Next operation.
*/
object map_mapPartition {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[5]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    /*In first example, I have applied map() transformation on dataset distributed
    between 3 partitions so that you can see function is called 9 times.*/
    spark.sparkContext.parallelize(1 to 9, 3).map(x=>(x, "Hello")).collect().foreach(println)
    println("mapPartitions..")
    /* In second example, when we applied mapPartitions(), you will notice it ran
     3 times i.e. for each partition once. We had to convert string "Hello" into
     iterator because mapPartitions() takes iterator as input. */
    spark.sparkContext.parallelize(1 to 9, 3).mapPartitions(x=>(Array("Hello").iterator)).collect.foreach(println)
    println("Prints one partition of mapPartitions")
    /*In thirds step, I tried to get the iterator next value to show you the element.
      Note that next is always increasing value, so you can't step back. */
    spark.sparkContext.parallelize(1 to 9, 3).mapPartitions(x=>(List(x.next).iterator)).collect.foreach(println)
    println("Prints all partitions and a false value as last hasNext doesnt have any 4th data in a partition")
    spark.sparkContext.parallelize(1 to 9, 3).mapPartitions(x=>(List(x.next, x.next, x.next, x.hasNext).iterator)).collect.foreach(println)

    val data = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8), 2)
    /*Partition*/
    def sumfuncmap(numbers : Int) : Int =
    {
      var sum = 1
      return sum + numbers
    }
    data.map(sumfuncmap).collect.foreach(print)
    println("MapPartition")
    /*mapPartition - applied on each partition*/
    def sumfuncpartition(numbers : Iterator[Int]) : Iterator[Int] =
    {
      var sum = 1
      while(numbers.hasNext)
      {
        sum = sum + numbers.next()
      }
      return Iterator(sum)
    }
    // returns 11 and 27 i.e 1+2+3+4+(1) =11 and 5+6+7+8+9+(1) =27
    data.mapPartitions(sumfuncpartition).collect.foreach(println)
    spark.stop();
  }
}
