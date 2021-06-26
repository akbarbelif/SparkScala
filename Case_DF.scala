package Others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object Case_DF {
    def main(args:Array[String]): Unit = {
      val spark = SparkSession
        .builder()
        .master("local")
        .appName("Case_If_statement")
        .getOrCreate()

      import spark.sqlContext.implicits._
      val data = List(("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0))

      val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
      val df = spark.createDataFrame(data).toDF(cols:_*)
      println("When Otherwise")
      val df2 = df.withColumn("new_gender",when(col("gender")==="M","Male")
                                                   .when(col("gender") === "F","Female")
                                                    .otherwise("Unknown"))
      df2.show(2)
      println("Case Statement")
      val df3 = df.withColumn("new_gender",
        expr("case when gender = 'M' then 'Male' " +
          "when gender = 'F' then 'Female' " +
          "else 'Unknown' end"))
      println(df3)
    }
}
