package Others
import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession

object Rolling_DateDiff_ex {
  def main(args: Array[String]) {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val spark = SparkSession
      .builder
      .config("spark.master", envProps.getString("executionMode"))
      .appName("MnMCount")
      .getOrCreate()
    import spark.implicits._
    val df = Seq(
      (1, 11, Timestamp.valueOf("2018-06-01 12:00:00")),
      (1, 12, Timestamp.valueOf("2018-06-01 13:00:00")),
      (1, 12, Timestamp.valueOf("2018-06-01 15:00:00")),
      (1, 12, Timestamp.valueOf("2018-06-01 17:00:00")),
      (1, 13, Timestamp.valueOf("2018-06-01 19:00:00")),
      (1, 13, Timestamp.valueOf("2018-06-01 20:00:00")),
      (2, 21, Timestamp.valueOf("2018-06-01 12:00:00")),
      (2, 21, Timestamp.valueOf("2018-06-01 17:00:00"))
    ) toDF("main_id", "sub_id", "time")

    val windows = Window.partitionBy($"main_id").orderBy($"main_id")
    df.withColumn("diff",
      (unix_timestamp($"time") - unix_timestamp(lag($"time", 1).over(windows))) / 3600.0
    ).show
  }
}
