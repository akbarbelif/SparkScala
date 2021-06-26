import org.apache.spark.sql.SparkSession
object Spark_Learning_Chapter4_Spark_SQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkSQLExampleApp")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()
    // Path to data set
    val csvFile="/home/bose/Downloads/SparkLearning/src/main/scala/data/departuredelays.csv"
    // Read and create a temporary view
    // Infer schema (note that for larger files you may want to specify the schema)
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")
    //val schema = "date STRING, delay INT, distance INT,origin STRING, destination STRING"
    spark.sql("""SELECT distance, origin, destination
                        FROM us_delay_flights_tbl WHERE distance > 1000
                        ORDER BY distance DESC""").show(10)
    println("we’ll find all flights between San Francisco (SFO) and Chicago\n(ORD) with at least a two-hour delay")
    spark.sql("""SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)


    spark.sql("""SELECT delay, origin, destination,
                                CASE
                                WHEN delay > 360 THEN 'Very Long Delays'
                                WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                                WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                                WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                                WHEN delay = 0 THEN 'No Delays'
                                ELSE 'Early'
                                END AS Flight_Delays
                           FROM us_delay_flights_tbl
                           ORDER BY origin, delay DESC""").show(10)

    //spark.sql("CREATE DATABASE learn_spark_db")
    //Creating a managed table
    //spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,
    //  distance INT, origin STRING, destination STRING)")
    println("Creating an unmanaged table...........")
    println("Dropping us_delay_flights_tbl_new")
    spark.sql("DROP TABLE IF EXISTS us_delay_flights_tbl_new")
    spark.sql("DROP TABLE IF EXISTS us_delay_flights_tbl_direct_creation")
    println("Creating table with data us_delay_flights_tbl_new")
    spark.sql("""CREATE TABLE us_delay_flights_tbl_new(date STRING, delay INT,
    distance INT, origin STRING, destination STRING)
    USING csv OPTIONS (PATH
    '/home/bose/Downloads/SparkLearning/src/main/scala/data/departuredelays.csv')""")
    spark.sql("SELECT * FROM us_delay_flights_tbl_new").show(5)

    println("Now creating table from data...")
    df.write
      .option("path", "/home/bose/Downloads/SparkLearning/src/main/scala/data/tables/us_flights_delay")
      .saveAsTable("us_delay_flights_tbl_direct_creation")

    spark.sql("SELECT * FROM us_delay_flights_tbl_direct_creation").show(5)

    println("Dropping existing view")
    spark.sql("DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view") //global view
    spark.sql("DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view")

    /* we cn also use below to drop views
    spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
    */

    println("Now creating view...")
    //global temp view created in global_temp schema
    spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS\nSELECT date, delay, origin, destination from us_delay_flights_tbl WHERE\norigin = 'SFO'")
    spark.sql("CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS\nSELECT date, delay, origin, destination from us_delay_flights_tbl WHERE\norigin = 'JFK'")

    spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").show(5)

    println("Metadata...")
    println("Database Names..")
    spark.catalog.listDatabases().show()
    println("Table Names..")
    spark.catalog.listTables().show()
    println("Column Names Of us_delay_flights_tbl..")
    spark.catalog.listColumns("us_delay_flights_tbl").show()
  }
}
