package Others

import MyExceptions.MyExceptionHandler
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object Exception_Handling_Ex {
   def main(args:Array[String]): Unit = {
     val spark = SparkSession
       .builder()
       .master("local")
       .appName("Exception_Handling")
       .getOrCreate()

     val inputDataWithDups = spark.read
       .option("header",true)
       .option("delimiter",",")
       .csv("/home/bose/Downloads/SparkLearning/src/main/scala/data/unique_data.csv")

     val inputDataDups = spark.read
       .option("header",true)
       .option("delimiter",",")
       .csv("/home/bose/Downloads/SparkLearning/src/main/scala/data/non_unique_data.csv")

     def validateDF (df: DataFrame) : DataFrame = {
       if (df.dropDuplicates(Seq("Emp_Name","Emp_Id")).count() != df.count()) {
         throw new MyExceptionHandler("Duplicate Data Found in Empid and EmpName now exiting")
       }
       df
     }
//     validateDF(inputDataDups).show()

     /*To view cleaner exception*/
     val finalDF = Try(validateDF(inputDataDups))

     import spark.implicits._
     finalDF match {
       case Success(df) => df.show()
       case Failure(exception) => println(s"Failed with error: ${exception.getMessage}")
     }

     /*Finding if there is duplicate in a dataframe*/
     println(Try(validateDF(inputDataDups)).isFailure) // Returns true when it has duplicate
   }
}
