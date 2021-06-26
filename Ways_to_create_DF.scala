package Others

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Ways_to_create_DF  {
  def main(args:Array[String]) {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("WaysToCreateDF")
      .getOrCreate()

    import spark.implicits._
    val columns = Seq("language", "user_count")
    val data = Seq(("Java", 2000), ("Scala", 7000), ("Python", 9809))

    /*RDD to DF*/
    val rdd = spark.sparkContext.parallelize(data)
    println("RDD to DF....")
    val dfFromRDD1 = rdd.toDF("language", "user_count")
    println(dfFromRDD1.printSchema())
    println("Spark Session to DF using createDataFrame")
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
    println(dfFromRDD2.printSchema())
    /*createDataFrame from SparkSession*/
    println("createDataFrame from SparkSession")
    var dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
    println(dfFromData2.printSchema())
    println("Rename columns")
    val data1 = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema1 = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val dfRenameColumn = spark.createDataFrame(spark.sparkContext.parallelize(data1),schema1)
    val dfRenameColumn1= dfRenameColumn.withColumnRenamed("dob","DateOfBirth")
    println(dfRenameColumn1.printSchema())
    println("Renaming multiple columns")
    val df2 = dfRenameColumn.withColumnRenamed("dob","DateOfBirth")
      .withColumnRenamed("salary","salary_amount")
    println(df2.printSchema())
    println("Rename nested elements...")
    val schema2 = new StructType()
      .add("fname",StringType)
      .add("middlename",StringType)
      .add("lname",StringType)

    dfRenameColumn.select(col("name").cast(schema2),
                          col("dob"),
                          col("gender"),
                          col("salary"))
                        .printSchema()
    println("Now we are updating nested table to flattened")
    dfRenameColumn.select(col("name.firstname").as("fname"),
      col("name.middlename").as("mname"),
      col("name.lastname").as("lname"),
      col("dob"),col("gender"),col("salary"))
      .printSchema()

    println("Now we rename column , also flattens")
    val df4 = dfRenameColumn.withColumn("fname",col("name.firstname"))
      .withColumn("mname",col("name.middlename"))
      .withColumn("lname",col("name.lastname"))
      .drop("name")
    df4.printSchema()

  }
}
