import org.apache.spark.sql.SparkSession
object Spark_Learning_Chapter4_DatSources {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DiffDataSources")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()
    // Use Parquet
    val file = """/home/bose/Downloads/SparkLearning/src/main/scala/data/2010-summary.parquet"""
    val df = spark.read.format("parquet").load(file)
    println("Parquet Fetching Another Way1...")
    df.show(5)
    // Use Parquet; you can omit format("parquet") if you wish as it's the default
    println("Parquet Fetching Another Way2...")
    val df2 = spark.read.load(file)
    df2.show(5)
    // Csv as datasource
    println("Csv as datasource...")
    val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load("/home/bose/Downloads/SparkLearning/src/main/scala/data/csv/*")
    df3.show(5)
    println("Json as datasource...")
    val df4 = spark.read.format("json")
      .load("/home/bose/Downloads/SparkLearning/src/main/scala/data/json")
    df4.show(5)
    println("drop view us_delay_flights_tbl_parquet")
    spark.sql("DROP VIEW IF EXISTS us_delay_flights_tbl_parquet")
    println("Creating view us_delay_flights_tbl_parquet")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl_parquet USING parquet OPTIONS (path '/home/bose/Downloads/SparkLearning/src/main/scala/data/2010-summary.parquet')")
    spark.sql("SELECT * FROM us_delay_flights_tbl_parquet").show(5)
    println("Writing to parquet file")
    df3.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("/home/bose/Downloads/SparkLearning/src/main/scala/data/parquet/df3_parquet")
    println("Reading csv...")
    val file_csv = "/home/bose/Downloads/SparkLearning/src/main/scala/data/csv/*"
    val schema_csv = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

    val df_csv = spark.read.format("csv")
      .schema(schema_csv)
      .option("header", "true")
      .option("mode", "FAILFAST")      // Exit if any errors
      .option("nullValue", "")         // Replace any null data with quotes
      .load(file_csv)
     df_csv.show(5)

    println("Creating view from csv...")
    //need to check the quote issue below
    //val query_vw=" CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl_csv_vw USING csv  OPTIONS ( path \" /home/bose/Downloads/SparkLearning/src/main/scala/data/csv/* \" , header \"true\" , inferSchema \"true\" , mode \"FAILFAST\") "
    //spark.sql(query_vw)
    //spark.sql("SELECT * FROM us_delay_flights_tbl_csv_vw").show(5)
    println("Creating write overwrite path from csv...")
    df.write.format("csv").mode("overwrite").save("/home/bose/Downloads/SparkLearning/src/main/scala/data/df_csv")
    println("Reading Avro File")
    //val df_avro = spark.read.format("avro").load("/home/bose/Downloads/SparkLearning/src/main/scala/data/avro/*")
    //df_avro.show(5)
    println("Reading ORC File")
    val file_orc = "/home/bose/Downloads/SparkLearning/src/main/scala/data/orc/*"
    val df_orc = spark.read.format("orc").load(file_orc)
    df_orc.show(10, false)
    println("Reading image...")
    val imageDir = "/home/bose/Downloads/SparkLearning/src/main/scala/data/train_images/"
    val imagesDF = spark.read.format("image").load(imageDir)
    println(imagesDF.printSchema)
    imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode",
      "label").show(5, false)
    //till page104
  }
}
