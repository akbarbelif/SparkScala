package Others

import org.apache.spark.sql.SparkSession

object Handling_Error_Record {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Handling error record")
    .getOrCreate()

    def main(args:Array[String]): Unit = {
      val data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')
    println("To include this data in a separate column...........")
      val corruptDf = spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(spark.sparkContext.parallelize(data))
    /*Below is corrupt record*/
    println("Below is corrupt record...")
    corruptDf.filter("_corrupt_record is not null").show()
    println("Below is proper record...")
      corruptDf.filter("_corrupt_record is null").show()

      println("To ignore all bad records ..................")
      val corruptDf1 = spark.read.option("mode", "DROPMALFORMED")
        .json(spark.sparkContext.parallelize(data))
      corruptDf1.show()
      println("Throws an exception when it meets corrupted records....")
      try {
        val corruptDf2 = spark.read
          .option("mode", "FAILFAST")
          .json(spark.sparkContext.parallelize(data))

      } catch {
        case e:Exception => print(e)
      }
    }
}
