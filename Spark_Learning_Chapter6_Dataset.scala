import org.apache.spark.sql.SparkSession

object Spark_Learning_Chapter6_Dataset {
  case class Bloggers(id:Int, first:String, last:String, url:String, date:String,
                      hits: Int, campaigns:Array[String])
  // Our case class for the Dataset
  case class Usage(uid:Int, uname:String, usage: Int)
  // Create a new case class with an additional field, cost
  case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DataSet Chapter6")
      .config("spark.master", "local") //remove in cluster
      .getOrCreate()
    /*val bloggers = "../data/bloggers.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]*/
    import scala.util.Random._
    val r = new scala.util.Random(42)
    // Create 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))
    // Create a Dataset of Usage typed data
    import spark.implicits._  //This was missing in book code
    val dsUsage = spark.createDataset(data) //creating dataset
    dsUsage.show(10)
    println("Now there is filter on the dataset created..")
    import org.apache.spark.sql.functions._
    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    println("Filter Added with Function In Dataset..")
    def filterWithUsage(u: Usage) = u.usage > 900
    dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)
    //returning a new Dataset of type Usage for rows
    //where the value of the expression or function is true .

    println("Use an if-then-else lambda expression and compute a value:")
    // Use an if-then-else lambda expression and compute a value
      dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
        .show(5, false)

    println("Define a function to compute the usage")
    // Define a function to compute the usage
    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.50
    }
    // Use the function as an argument to map()
    dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

    println("We compute and also bring all columns")

    // Compute the usage cost with Usage as a parameter
    // Return a new object, UsageCost
    def computeUserCostUsage(u: Usage): UsageCost = {
      val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
      UsageCost(u.uid, u.uname, u.usage, v)  //v is cost field
    }
    // Use map() on our original Dataset
    dsUsage.map(u => {computeUserCostUsage(u)}).show(5)

    //Below is used to convert DF TO DS ,the DS having Bloggers case class
    /*
    spark.read.format("json") returns a DataFrame<Row> , which in Scala is a type alias
for Dataset[Row] . Using .as[Bloggers] instructs Spark to use encoders, discussed
later in this chapter, to serialize/deserialize objects from Spark’s internal memory rep‐
resentation to JVM Bloggers objects.
    */
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", "/data/bloggers/bloggers.json")
      .load()
      .as[Bloggers]
    //done till page -173
  }
}
