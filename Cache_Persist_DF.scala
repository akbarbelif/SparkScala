package Others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

object Cache_Persist_DF {
   def main(args:Array[String]): Unit = {
     val spark = SparkSession
       .builder()
       .master("local")
       .appName("Cache&Persist")
       .getOrCreate()

     val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
       .csv("/home/bose/Downloads/SparkLearning/src/main/scala/data/uszips.csv")

     val df2 = df.where(col("state_id") === "PR").cache()
     df2.show(false)

     println(df2.count())

     /*MANUALLY REMOVING PERSISTED DATA ELSE WE WILL GET WARN (WARN CacheManager: Asked to cache already cached data.)*/
    // val dfPersistU = df.unpersist()

     println("Persistence Default...")
     val dfPersist = df.persist()  //default is MEMORY_AND_DISK
     println(dfPersist)

     //val dfPersistU1 = df.unpersist()

     println("Persistence MEMORY_ONLY...")
     val dfPersist1 = df.persist(StorageLevel.MEMORY_ONLY)
     println(dfPersist1)
   }
}
