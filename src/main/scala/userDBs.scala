package UserDatabase

import java.time._
//import org.joda.time.DateTime
//import org.joda.time.Days

//FIXME


import org.apache.log4j.Logger

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
  * Determine unique integer id for users and items.
  * Aggregation of ratings by user, 
  *   applying a penalty of 0.95 to each day of difference from max date
  * Apply a filter of ratings > 0.01
  */
object ImportUser {

   def main(args: Array[String]) : Unit = {
      val inputPath = args(0)

      val spark = SparkSession.builder
         .master("local[*]")
         .appName("USER-DB")
         .getOrCreate()

import spark.implicits._

      val schema = StructType(Seq(
               StructField("userId"   , StringType, true),
               StructField("itemId"   , StringType, true),
               StructField("rating"   , DoubleType, true),
               StructField("timestamp", LongType  , true)
               )
            )

      val df_user_item = spark.read
         .format("com.databricks.spark.csv")
         .option("header", false)
         .schema(schema)
         .load(inputPath)


     /**********************************************************
       Assign a unique integer number to a userId string
       *********************************************************/
     val uniqueUsers = df_user_item.select("userID")
         .distinct()
         .rdd.zipWithIndex
         .map {
            case (uid: Row, idx: Long) => (uid(0).toString, idx)
         }
         .toDF("userId", "userIdAsInteger")

     //uniqueUsers.show(10)
     uniqueUsers
       .repartition(1) //this is to write the output into one single file
                       //could be dangerous if the data is big, 
                       //since the process will collect the data to the drive, for instance. 
                       //There will be easily a problem of OOM
       .write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode(SaveMode.Overwrite)
       .save("output/lookupUser.csv")

     println("nb of unique users: " + uniqueUsers.count)

     /**********************************************************
       Assign a unique integer number to a userId string
       *********************************************************/
     val uniqueItems = df_user_item.select("itemId")
         .distinct()
         .rdd.zipWithIndex
         .map {
            case (iid: Row, idx: Long) => (iid(0).toString, idx)
         }
         .toDF("itemId", "itemIdAsInteger")

     println("nb of unique items: " + uniqueItems.count)

     uniqueItems
         .repartition(1) //same as above
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode(SaveMode.Overwrite)
         .save("output/lookupProduct.csv")

     /*********************************************************
       Normalize ratings by applying a factor of 0.95
       to each day of difference to the last day (max timestamp)
       ********************************************************/
     val timestamp_max = df_user_item
         .agg(max("timestamp")).first()(0).asInstanceOf[Long]
     println("timestamp_max = " + timestamp_max)

     val penaltyFactor = 0.95d

     def getPenalizedScore(timeStamp: Long, rating: Double) : Double = {
        val timeGapInDays = (timestamp_max - timeStamp) / (1000.0 * 3600.0 * 24)
        rating * scala.math.pow(penaltyFactor, timeGapInDays)
     }

     // using a udf (user defined function) ---
     val udf_getPenalizedScore = udf((timestamp: Long, rating: Double) => getPenalizedScore(timestamp, rating), DoubleType)

     val df_user_item_norm = df_user_item
         //apply the previous udf on the column "rating",
         // create new one named "normRating"
         .withColumn("normRating", 
            udf_getPenalizedScore(col("timestamp"), col("rating")))
         .drop("rating")



     /*********************************************************
       Construct aggregated ratings
       ********************************************************/

     val aggRatings = df_user_item_norm
         // get corresponding "userIdAsInteger" from table uniqueUsers
         .join(uniqueUsers, Seq("userId"), "left_outer")

         // get corresponding "itemIdAsInteger" from table uniqueUsers
         .join(uniqueItems, Seq("itemId"), "left_outer")
         .drop("userId", "itemId")

         .groupBy("userIdAsInteger", "itemIdAsInteger")
         .agg(expr("sum(normRating) as sumRating"))

         //apply filter to keep only ratings > 0.01
         .filter(col("sumRating") > lit(0.01))
     //aggRatings.show(10)

     //save aggregate ratings to csv
     aggRatings
         .repartition(1) //FIXME
	      .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode(SaveMode.Overwrite)
         .save("output/agg_ratings.csv")
 
   }
}

