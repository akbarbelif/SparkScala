import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
object Spark_Learning_Chapter3_Dataset {
  case class DeviceIoTData (battery_level: Long, c02_level: Long,
                            cca2: String, cca3: String, cn: String, device_id: Long,
                            device_name: String, humidity: Long, ip: String, latitude: Double,
                            lcd: String, longitude: Double, scale:String, temp: Long,
                            timestamp: Long)
  case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
                                 cca3: String)
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val spark = SparkSession
      .builder
      .appName("Dataset_Example")
      .config("spark.master", envProps.getString("executionMode")) //remove in cluster
      .getOrCreate()
    if (args.length <= 0) {
      println("usage Example3_7 <file path to blogs.json>")
      System.exit(1)
    }
    import spark.implicits._

    //Dataset[Row] returned while we read a file is converted to DataSet[DeviceIoTData]
    val ds   = spark.read
      .json(envProps.getString("chapter3_json_iot"))  //path is in config
      .as[DeviceIoTData]
    ds.show(5, false)
    println("DataSet Operation Filter....")
    /*
    The version we used, filter(func: (T) > Boolean): Dataset[T] , takes a lambda function, func: (T) >
    Boolean , as its argument.The argument to the lambda function is a JVM object of type DeviceIoTData . As
such, we can access its individual data fields using the dot ( . ) notation, like you
would in a Scala class or JavaBean.
    */
    val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})
      filterTempDS.show(5, false)

    /*Below we use case class DeviceTempByCountry*/
    println("map with DeviceTempByCountry...")
    val dsTemp = ds
      .filter(d => {d.temp > 25})
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]

    dsTemp.show(5, false)
    println("inspect only the first row of your Dataset...")
    val device = dsTemp.first()
    println(device)

    /*same query using column names and then cast to a Dataset[DeviceTempByCountry]*/
    println("same query using column names and then cast to\na Dataset[DeviceTempByCountry] ...")
    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
      .where("temp > 25")
      .as[DeviceTempByCountry]
    println(dsTemp2)
    //till page-85
  }
}
