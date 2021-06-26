import org.apache.spark.sql.SparkSession

import scala.util.Random
object Spark_Learning_Chapter7_Joins {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Joins")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    // Generate some sample data for two data sets
    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)

    // Initialize states and items purchased
    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4",
      5-> "SKU-5")
    import spark.implicits._
    // Create DataFrames
    val usersDF = (0 to 10).map(id => (id, s"user_${id}",
      s"user_${id}@databricks.com", states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")
    val ordersDF = (0 to 100)
      .map(r => (r, r, rnd.nextInt(10), 10 * r* 0.2d,
        states(rnd.nextInt(5)), items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    // Do the join
    val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")
    // Show the joined results
    usersOrdersDF.show(false)
  }
}
