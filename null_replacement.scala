package Others

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object null_replacement {
   def main(args: Array[String]):Unit = {
     val props = ConfigFactory.load()
     val envProps = props.getConfig(args(0))
     val spark = SparkSession
       .builder
       .appName("Null Replacement")
       .config("spark.master", envProps.getString("executionMode")) //remove in cluster
       .getOrCreate()
     val nullcontaingdf="/home/bose/Downloads/SparkLearning/src/main/scala/data/null_data_handling.csv"
     val data_with_null = spark.read.option("inferSchema","true")
       .option("header", "true")
       .option("treatEmptyValuesAsNulls", "true") //replace empty values with null
       .csv(nullcontaingdf)
     //replace null values with value
     // data_with_null.na.fill("NULL IN SOURCE").show(5)
     data_with_null.show(5)
     spark.stop();
   }
}
