package Others

import org.apache.spark.sql.SparkSession

object forEach_Accum_DF {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("forEach Example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")

    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

    df.foreach(f => {longAcc.add(f.getInt(5))})
    println("Accumulator Value to find sum of bonus: " +longAcc)

  }
}
