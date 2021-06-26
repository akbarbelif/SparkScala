package Others

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object FlatMap_DF {
   def main(args:Array[String]): Unit = {
     val spark = SparkSession
       .builder()
       .appName("FlatMap")
       .master("local")
       .getOrCreate()

     val arrayStructureData = Seq(
       Row("James,,Smith",List("Java","Scala","C++"),"CA"),
       Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
       Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
     )

     val arrayStructureSchema = new StructType()
       .add("name",StringType)
       .add("languagesAtSchool", ArrayType(StringType))
       .add("currentState", StringType)

     val df = spark.createDataFrame(
         spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema
     )
     df.show()
     import spark.implicits._
     val df2 = df.flatMap(f => f.getSeq[String](1).map((f.getString(0),_,f.getString(2))))
       .toDF("Name","language","State")
     df2.show(false)
   }
}
