package Others

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object Filter_DF {
    def main(args:Array[String]): Unit ={
      val spark = SparkSession
        .builder()
        .config("spark.master", "local")
        .appName("FilterDF")
        .getOrCreate()

      val arrayStructureData = Seq(
        Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
        Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
        Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
        Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
        Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
        Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
      )

      val arrayStructureSchema = new StructType()
        .add("name",new StructType()
          .add("firstname",StringType)
          .add("middlename",StringType)
          .add("lastname",StringType))
        .add("languages", ArrayType(StringType))
        .add("state", StringType)
        .add("gender", StringType)

      val dfNested = spark.createDataFrame(
        spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
      dfNested.printSchema()

      dfNested.filter(dfNested("state") === "OH").show(false)

      dfNested.filter("gender == 'M'").show(false)

      println("Filter on an Array Column")
      import org.apache.spark.sql.functions.array_contains
      dfNested.filter(array_contains(dfNested("languages"),"Java"))
        .show(false)
      println("Filter on Nested Struct columns")
      dfNested.filter(dfNested("name.lastname") === "Williams")
        .show(false)
    }
}
