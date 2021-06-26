import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config._
object Spark_Learning_Chapter3_json {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val spark = SparkSession
      .builder
      .appName("Example-3_7")
      .config("spark.master", envProps.getString("executionMode")) //remove in cluster
      .getOrCreate()
    if (args.length <= 0) {
      println("usage Example3_7 <file path to blogs.json>")
      System.exit(1)
    }
    // Get the path to the JSON file
    val jsonFile = envProps.getString("chapter3_json_blog")             // args(0)
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)
    println("Print the schema")
    println(blogsDF.printSchema)
    println(blogsDF.schema)
    println("prints column name")
    println(blogsDF.columns.toSeq) // prints column name
    println("example of expression")
    blogsDF.select(expr("Hits * 2")).show(2) //example of expression
    blogsDF.select(col("Hits") * 2).show(2)  // example of col
    println("Below Big Hitters is false where condn not met")
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show() //example of expression with filter
    println("Concatenate 3 columns using expr")
    blogsDF
      .withColumn("AuthorsId",(concat(expr("First"),expr("Last"),expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)
    println("expr is the same as a col method call")
    blogsDF.select(expr("Hits")).show(2)
    blogsDF.select(col("Hits")).show(2)
    blogsDF.select("Hits").show(2)
    println("Sort by column Id in descending order")
    blogsDF.sort(col("Id").desc).show()
    //blogsDF.sort($"Id".desc).show() make it working why not working
    println("Rows Example:")
    import org.apache.spark.sql.Row
    // Create a Row
    import org.apache.spark.sql.Row
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))
    // Access using index for individual items
    println(blogRow(1))
    println("Row objects can be used to create DataFrames")
    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    import spark.implicits._  // for toDF and show implicits._ needed
    val authorsDF = rows.toDF("Author", "State")
    println(authorsDF.show())

    val sampleDF = spark
      .read
      .option("samplingRatio", 0.001)
      .option("header", true)
      .csv("/home/bose/Downloads/SparkLearning/src/main/scala/Spark_Learning_Chapter2.scala")

    // In Scala it would be similar
    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
                                      StructField("UnitID", StringType, true),
                                      StructField("IncidentNumber", IntegerType, true),
                                      StructField("CallType", StringType, true),
                                      StructField("CallDate", StringType, true),
                                      StructField("WatchDate", StringType, true),
                                      StructField("CallFinalDisposition", StringType, true),
                                      StructField("AvailableDtTm", StringType, true),
                                      StructField("Address", StringType, true),
                                      StructField("City", StringType, true),
                                      StructField("Zipcode", IntegerType, true),
                                      StructField("Battalion", StringType, true),
                                      StructField("StationArea", StringType, true),
                                      StructField("Box", StringType, true),
                                      StructField("OriginalPriority", StringType, true),
                                      StructField("Priority", StringType, true),
                                      StructField("FinalPriority", IntegerType, true),
                                      StructField("ALSUnit", BooleanType, true),
                                      StructField("CallTypeGroup", StringType, true),
                                      StructField("NumAlarms", IntegerType, true),
                                      StructField("UnitType", StringType, true),
                                      StructField("UnitSequenceInCallDispatch", IntegerType, true),
                                      StructField("FirePreventionDistrict", StringType, true),
                                      StructField("SupervisorDistrict", StringType, true),
                                      StructField("Neighborhood", StringType, true),
                                      StructField("Location", StringType, true),
                                      StructField("RowID", StringType, true),
                                      StructField("Delay", FloatType, true)))

    // Read the file using the CSV DataFrameReader
    // Use the DataFrameReader interface to read a CSV file
    val sfFireFile="/home/bose/Downloads/SparkLearning/src/main/scala/data/sf-fire-calls.csv"
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    println("Where condition in scala")
    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where($"CallType" =!= "Medical Incident")
    fewFireDF.show(5, false)

    println("In Scala to save as a Parquet file- done execute again it will fail")
   // val parquetPath = "/home/bose/Downloads/SparkLearning/src/main/scala/data/output/fewFireDF.parquet"
   // fewFireDF.write.format("parquet").save(parquetPath)
    println("Distinct Count - CallType")
    import org.apache.spark.sql.functions._
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct('CallType) as 'DistinctCallTypes)
      .show()

    println("Distinct Values - CallType")
    fireDF
      .select("CallType")
     // .where($"CallType".isNotNull())
      .distinct()
      .show(10, false)

    println("Renaming Column - Delay to ResponseDelayedinMins")
    val newFireRenameDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireRenameDF
      .select("ResponseDelayedinMins")
      .where($"ResponseDelayedinMins" > 5)
      .show(5, false)

    println("String to Timestamp Conversion")
    val fireTsDF = newFireRenameDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    println("Order by year from full timestamp")
    fireTsDF
      .select(year($"IncidentDate"))
      .distinct()
      .orderBy(year($"IncidentDate"))
      .show()

    println("groupBy Example also isNotNull check")
    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    println("Max-Min-Average")
    import org.apache.spark.sql.{functions => F}
    fireTsDF
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
      .show()

    /*Till page-71*/
  }
}
