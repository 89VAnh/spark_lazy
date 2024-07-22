//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// -- Get the average score of comments for each user

object NonLazy10 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Non-lazy performance test 10")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the comments table into a DataFrame
    val commentsDF = spark.table("comments")

    // Perform the aggregation
    commentsDF.groupBy("_UserId").agg(avg("_Score").alias("average_score")).write.mode("overwrite").saveAsTable("comments_group")

    val comments_group = spark.table("comments_group")
    
    val result =   comments_group.orderBy(col("average_score").desc)

    // Show the result
    result.show()

    // Stop the Spark session
    spark.stop()
  }
}
