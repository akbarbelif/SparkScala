import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Spark_Learning_Chapter5_HOF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkHigherOrderFunction")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()
    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    import spark.implicits._
    val tC = Seq(t1, t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")
    println("Initially Value")
    tC.show()
    /*
    transform(): The transform() function produces an array by applying a function to each element
    of the input array (similar to a map() function):
    */
    // Calculate Fahrenheit from Celsius for an array of temperatures
    println("transform:Calculate Fahrenheit from Celsius for an array of temperatures")
    spark.sql("""
    SELECT celsius,
           transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
    FROM tC
    """).show()
    // Filter temperatures > 38C for array of temperatures
    println("filter:Filter temperatures > 38C for array of temperatures")
    spark.sql("""
    SELECT celsius,
    filter(celsius, t -> t > 38) as high
    FROM tC
    """).show()
    /*
    The exists() function returns true if the Boolean function holds for any element in
    the input array
    */
    // Is there a temperature of 38C in the array of temperatures
    println("exists:Is there a temperature of 38C in the array of temperatures")
    spark.sql("""
    SELECT celsius,
    exists(celsius, t -> t = 38) as threshold
    FROM tC
    """).show()
    // Calculate average temperature and convert to F
    println("reduce:Calculate average temperature and convert to F")
   /* spark.sql("""
                      SELECT celsius,
                      reduce(celsius,0,(t, acc) -> t + acc,acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit
                      FROM tC
                      """).show()*/

  }
}
