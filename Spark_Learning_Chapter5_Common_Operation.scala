import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Spark_Learning_Chapter5_Common_Operation {
  def main(args: Array[String]): Unit = {
    // Set file paths
    val delaysPath =
      "/home/bose/Downloads/SparkLearning/src/main/scala/data/departuredelays.csv"
    val airportsPath =
      "/home/bose/Downloads/SparkLearning/src/main/scala/data/airport-codes-na.txt"
    val spark = SparkSession
      .builder
      .appName("SparkSQL_CommonOperations")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()

    println("Dropping the temp view")
    spark.catalog.dropTempView("airports_na")
    spark.catalog.dropTempView("departureDelays")
    spark.catalog.dropTempView("foo")

    println("Create temp view airports_na")
    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")

    println("Create temp view departureDelays")
    // Obtain departure Delays data set
    val delays = spark.read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Create temporary small table
    println("Create temporary small table")
    val foo = delays.filter(
      expr(
        """origin == 'SEA' AND destination == 'SFO' AND
      date like '01010%' AND delay > 0"""))
    foo.createOrReplaceTempView("foo")

    println("Reading airports_na..")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    println("Reading departureDelays..")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    println("Reading foo..")
    spark.sql("SELECT * FROM foo").show()

    println("Union Example")
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr(
      """origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '01010%' AND delay > 0""")).show()

    println("Now printing union dataframe named bar")
    spark.sql(
      """
              SELECT *
              FROM bar
              WHERE origin = 'SEA'
              AND destination = 'SFO'
              AND date LIKE '01010%'
              AND delay > 0
              """).show()

    println("Join Operation...")
    /*  foo.join(
      airports.as('air),
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination").show()

   */

    spark.sql(
      """
                SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
                FROM foo f
                JOIN airports_na a
                ON a.IATA = f.origin
                """).show()

    /* println("Windowing Function...")
    spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")
    println("Creating table departureDelaysWindow")
    spark.sql("CREATE TABLE departureDelaysWindow AS SELECT origin, destination, SUM(delay) AS TotalDelays FROM departureDelays WHERE origin IN ('SEA', 'SFO', 'JFK') AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') GROUP BY origin, destination")
    spark.sql("SELECT * FROM departureDelaysWindow")
    println("Below is dense tank window function")
    spark.sql("""
              SELECT origin, destination, TotalDelays, rank
              FROM (
              SELECT origin, destination, TotalDelays, dense_rank()
              OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
              FROM departureDelaysWindow
              ) t
              WHERE rank <= 3
              """).show()
              */
    println("Adding a column")
    import org.apache.spark.sql.functions.expr
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )
    foo2.show()
    println("Dropping a column")
    val foo3 = foo2.drop("delay")
    foo3.show()
    println("Renaming columns")
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()
    println("Normal table ..")
    spark.sql("SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'")
    println("Table pivoted now ..")
    spark.sql("SELECT * FROM (SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA')PIVOT (CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay FOR month IN (1 JAN, 2 FEB)) ORDER BY destination").show()
  }
}