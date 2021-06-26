package Others

import org.apache.spark.sql.SparkSession

object groupBy_DF {
  def main(args:Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .master("local")
      .appName("Pivot")
      .getOrCreate()
    import spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.show()
    println("Group By ...")
    df.groupBy("department").sum("salary").show(false)
    println("Department Count per group")
    df.groupBy("department").count().show()
    println("Aggregation Together")
    import org.apache.spark.sql.functions._
    df.groupBy("department")
      .agg(
           sum("salary").as("sum_salary"),
           avg("salary").as("avg_salary"),
           sum("bonus").as("sum_bonus"),
           avg("bonus").as("avg_bonus")
      )
      .show(false)
    println("Filter On Aggregation")
    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus"))
      .where(col("sum_bonus") >= 50000)
      .show(false)
  }
}
