package Others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Pivot_DF {
  def main(args:Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .master("local")
      .appName("Pivot")
      .getOrCreate()

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    println("Pivot..")
    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    pivotDF.show()

    println("Unpivot")
    //unpivot
    val unPivotDF = pivotDF.select($"Product",
      expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))
      .where("Total is not null")
    unPivotDF.show()
  }
}
