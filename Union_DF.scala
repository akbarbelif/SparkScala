package Others

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
/*union - by sequence also datatype should be compatible */
object Union_DF {
   def main(args:Array[String]): Unit = {
     val spark = SparkSession
       .builder()
       .appName("Union DF")
       .master("local")
       .getOrCreate()

     import spark.implicits._

     val simpleData = Seq(("James","Sales","NY",90000,34,10000),
       ("Michael","Sales","NY",86000,56,20000),
       ("Robert","Sales","CA",81000,30,23000),
       ("Maria","Finance","CA",90000,24,23000)
     )
     val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
     df.printSchema()
     df.show()

     val simpleData2 = Seq((10000,"Sales","NY",90000,34,"James"),
       (23000,"Finance","CA",90000,24,"Maria"),
       (5000,"Finance","NY",79000,53,"Jen"),
       (18000,"Marketing","CA",80000,25,"Jeff"),
       (21000,"Marketing","NY",91000,50,"Kumar")
     )
     val df2 = simpleData2.toDF("bonus","department","state","salary","age","employee_name")
     df2.printSchema()
     df2.show()
     println("Union of two df")
     val df3 = df.union(df2)
     df3.show(false)
     df3.printSchema()
     /*Above sequence is wrong so we use below*/
     println("UnionByName union by name...")
     /*def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
          val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
          a.select(columns: _*).unionAll(b.select(columns: _*))
        }*/
     val dfu=df.unionByName(df2)
     dfu.show()
     println("Union without duplicate")
     val df5 = df.union(df2).distinct()
     df5.show(false)
     df5.printSchema()
   }
}
