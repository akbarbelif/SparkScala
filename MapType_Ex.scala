package Others

import org.apache.spark.sql.functions.{col, map_concat, map_keys, map_values}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType, StringType, StructType}

object MapType_Ex {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MapType Ex")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row("James",List(Row("Newark","NY"),Row("Brooklyn","NY")),
        Map("hair"->"black","eye"->"brown"), Map("height"->"5.9")),
      Row("Michael",List(Row("SanJose","CA"),Row("Sandiago","CA")),
        Map("hair"->"brown","eye"->"black"),Map("height"->"6")),
      Row("Robert",List(Row("LasVegas","NV")),
        Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")),
      Row("Maria",null,Map("hair"->"blond","eye"->"red"),
        Map("height"->"5.6")),
      Row("Jen",List(Row("LAX","CA"),Row("Orange","CA")),
        Map("white"->"black","eye"->"black"),Map("height"->"5.2"))
    )

    val mapType  = DataTypes.createMapType(StringType,StringType)

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("addresses", ArrayType(new StructType()
        .add("city",StringType)
        .add("state",StringType)))
      .add("properties", mapType)
      .add("secondProp", MapType(StringType,StringType))

    val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    mapTypeDF.printSchema()
    mapTypeDF.show()
    println("Getting all map Keys from DataFrame MapType column..")
    mapTypeDF.select(col("name"),map_keys(col("properties"))).show(false)
    println("Getting all map values from the DataFrame MapType column")
    mapTypeDF.select(col("name"),map_values(col("properties"))).show(false)
    println("Merging maps using map_concat()")
    mapTypeDF.select(col("name"),map_concat(col("properties"),col("secondProp"))).show(false)
   /*Convert an array of StructType entries to map*/
    /*Use map_from_entries() SQL function to convert array of StructType entries to map (MapType) on Spark DataFrame.*/
    
  }
}
