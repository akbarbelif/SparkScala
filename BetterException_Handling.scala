package Others

import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter, StringWriter}
import scala.util.{Try, Success, Failure}
//import scala.util.control.NonFatal
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
/*Each statement is within try and catch and logged in a file*/
object BetterException_Handling {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Exception_Handling")
      .getOrCreate()

    val erroutput = new StringWriter
    val todays_trans_dt = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance.getTime)
    val datarecord = ""

    val inputDataWithDups = spark.read
      .option("header",true)
      .option("delimiter",",")
      .csv("/home/bose/Downloads/SparkLearning/src/main/scala/data/unique_data.csv")

    try {
      val inputDataDups = spark.read
        .option("header", true)
        .option("delimiter", ",")
        .csv("/home/bos/Downloads/SparkLearning/src/main/scala/data/non_unique_data.csv")
    Success(inputDataDups)
    } catch {
      case d: Throwable => d.printStackTrace(new PrintWriter(erroutput))
        new PrintWriter(s"/home/bose/Downloads/SparkLearning/src/main/scala/logs/${datarecord}_'BetterException'_$todays_trans_dt.log") //Saves error message to this location
        {
          write(erroutput.toString);

          close
        }
        Failure(d)
    }
    // Again next code here
    try {
      val inputDataDups = spark.read
        .option("header", true)
        .option("delimiter", ",")
        .csv("/home/bos/Downloads/SparkLearning/src/main/scala/data/non_unique_data.csv")
      Success(inputDataDups)
    } catch {
      case d: Throwable => d.printStackTrace(new PrintWriter(erroutput))
        new PrintWriter(s"/home/bose/Downloads/SparkLearning/src/main/scala/logs/${datarecord}_'BetterException'_$todays_trans_dt.log") //Saves error message to this location
        {
          write(erroutput.toString);

          close
        }
        Failure(d)
    }
  }
}
