package Others
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.functions._
object explode_example {
     def main(args: Array[String]): Unit = {

       val arrayData = Seq(
         Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
         Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
         Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
         Row("Washington",null,null),
         Row("Jefferson",List(),Map())
       )

       val arraySchema = new StructType()
         .add("name",StringType)
         .add("knownLanguages", ArrayType(StringType))
         .add("properties", MapType(StringType,StringType))
       val spark = SparkSession.builder()
         .master("local[5]")
         .appName("Explode example")
         .getOrCreate()
       import spark.implicits._
       val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
       df.printSchema()
       df.show(false)

       df.select($"name",explode($"knownLanguages"))
         .show(false)
       /*Spark SQL explode_outer(e: Column) function
        is used to create a row for each element in the array or map column*/
       println("explode_outer")
       df.select($"name",explode_outer($"properties"))
         .show()
     }
}
