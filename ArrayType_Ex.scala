package Others

import org.apache.spark.sql.functions.{array, array_contains, explode}
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object ArrayType_Ex {
   def main(args: Array[String]): Unit ={

     val spark = SparkSession
       .builder()
       .master("local")
       .appName("ArrayType")
       .getOrCreate()

     val arrayStructureData = Seq(
       Row("James,,Smith",List("Java","Scala","C++"),List("Spark","Java"),"OH","CA"),
       Row("Michael,Rose,",List("Spark","Java","C++"),List("Spark","Java"),"NY","NJ"),
       Row("Robert,,Williams",List("CSharp","VB"),List("Spark","Python"),"UT","NV")
     )

     val arrayStructureSchema = new StructType()
       .add("name",StringType)
       .add("languagesAtSchool", ArrayType(StringType))
       .add("languagesAtWork", ArrayType(StringType))
       .add("currentState", StringType)
       .add("previousState", StringType)

     val df = spark.createDataFrame(
       spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
     df.printSchema()
     df.show()

     /*explode*/
     println("Explode...")
     import spark.implicits._
     df.select($"name",explode($"languagesAtSchool")).show(false)
     println("Split...")
     df.select(functions.split($"name",",").as("nameAsArray") ).show(false)
     println("Concat 2 columns into another array column..")
     df.select($"name",array($"currentState",$"previousState").as("States") ).show(false)
     println("Searching in array")
     df.select($"name",array_contains($"languagesAtSchool","Java")
       .as("array_contains")).show(false)
   }
}
