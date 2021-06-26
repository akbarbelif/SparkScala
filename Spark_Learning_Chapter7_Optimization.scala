import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.current_timestamp
object Spark_Learning_Chapter7_Optimization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DataSet Chapter6")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()
    val ds = spark.read.textFile("/home/bose/Downloads/SparkLearning/src/main/scala/data/README.md").repartition(16)
    println(ds.rdd.getNumPartitions)

    // Create a DataFrame with 10M records
   /* val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    println("Now caching...")
    println(current_timestamp())
    df.cache() // Cache the data
    df.count() // Materialize the cache
    println("Now getting from cache...")
    println(current_timestamp())
    df.count() // Now get it from the cache -takes less time
    println("Completed Fetching")
    println(current_timestamp())

    /*Persistence Example Below-Not excuting properly*/
    // In Scala
    import org.apache.spark.storage.StorageLevel
    // Create a DataFrame with 10M records
    val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    df.persist(StorageLevel.DISK_ONLY) // Serialize the data and cache it on disk
    df.count() // Materialize the cache
    //Command took 2.08 seconds
      df.count() // Now get it from the cache
    //Command took 0.38 seconds
    /*Below is example of caching table*/
    df.createOrReplaceTempView("dfTable")
    spark.sql("CACHE TABLE dfTable")
    spark.sql("SELECT count(*) FROM dfTable").show()*/
  }
}
