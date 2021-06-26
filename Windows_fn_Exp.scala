package Others
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object Windows_fn_Exp {
  case class Salary(depName: String, empNo: Long, salary: Long)
   def main(args:Array[String]): Unit = {

     val spark = SparkSession
       .builder()
       .master("local")
       .appName("Window Function")
       .getOrCreate()

     import spark.implicits._
     val empsalary = Seq(
       Salary("sales", 1, 5000),
       Salary("personnel", 2, 3900),
       Salary("sales", 3, 4800),
       Salary("sales", 4, 4800),
       Salary("personnel", 5, 3500),
       Salary("develop", 7, 4200),
       Salary("develop", 8, 6000),
       Salary("develop", 9, 4500),
       Salary("develop", 10, 5200),
       Salary("develop", 11, 5200)).toDF()

     val byDepName = Window.partitionBy("depName")
     val agg_sal = empsalary
       .withColumn("max_salary", max("salary").over(byDepName))
       .withColumn("min_salary", min("salary").over(byDepName))

     import spark.implicits._
     agg_sal.select("depname", "max_salary", "min_salary")
       .dropDuplicates()
       .show()

     println("Rank..")
     val winSpec = Window.partitionBy($"depName").orderBy($"salary".desc)
     val rank_df = empsalary.withColumn("rank",rank().over(winSpec))
     rank_df.show()

     println("Dense Rank..")
     val dense_rank_df = empsalary.withColumn("dense_rank", dense_rank().over(winSpec))
     dense_rank_df.show()

     println("row_number..")
     val row_num_df = empsalary.withColumn("row_number", row_number().over(winSpec))
     row_num_df.show()

      println("percentage_rank..")
      val percent_rank_df = empsalary.withColumn("percent_rank", percent_rank().over(winSpec))
      percent_rank_df.show()

     val winSpec3 = Window.partitionBy("depName").orderBy("salary")

     println("cum_dist ..")
     val cume_dist_df =
       empsalary.withColumn("cume_dist",cume_dist().over(winSpec3))

     cume_dist_df.show()

     println("Lag function: lag..")
     val winSpec1 = Window.partitionBy("depName").orderBy("salary")
     val lag_df =
       empsalary.withColumn("lag", lag("salary", 2).over(winSpec1))
     lag_df.show()

     println("Lead function: lead..")
     val winSpec2 = Window.partitionBy("depName").orderBy("salary")
     val lead_df =
       empsalary.withColumn("lead", lead("salary", 2).over(winSpec2))
     lead_df.show()
   }
}
