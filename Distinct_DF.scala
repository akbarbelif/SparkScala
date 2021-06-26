package Others

import org.apache.spark.sql.SparkSession

object Distinct_DF {
    def main(args:Array[String]): Unit = {
      val spark = SparkSession
        .builder()
        .master("local")
        .appName("Distinct")
        .getOrCreate()

      import spark.implicits._

      val simpleData = Seq(("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)
      )

      val df = simpleData.toDF("employee_name","department","salary")
      df.show()

      println("Finding Distinct...")
      val distinctDF = df.distinct()
      println("After distinct count is "+distinctDF.count())
      distinctDF.show()

      println("Alternate way to find distinct")
      val df2 = df.dropDuplicates()  //we can pass columns also here then only for those record will come
      println("Alternate distinct count is"+df2.count())
      df2.show()

      //Distinct using dropDuplicates
      val dropDisDF = df.dropDuplicates("department","salary")
      println("Distinct count of department & salary : "+dropDisDF.count())
      dropDisDF.show(false)
    }
}
