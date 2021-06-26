import org.apache.spark.sql.SparkSession

object Spark_Learning_Chapter5_udf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkUDF")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()
    // Create cubed function
    val cubed = (s: Long) => {
      s * s * s
    }
    // Register UDF
    spark.udf.register("cubed", cubed)
    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
  }
}
