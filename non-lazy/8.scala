//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NonLazy8 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Non-lazy performance test 8")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the tables into DataFrames
    val postsDF = spark.table("posts")
    val commentsDF = spark.table("comments")
      .drop("_creationdate")
      .withColumnRenamed("_id", "_commentid")
      .withColumnRenamed("_score", "_comment_score")

    // Join the posts and comments DataFrames and save as table
    postsDF.join(commentsDF, postsDF("_Id") === commentsDF("_PostId"))
      .write.mode("overwrite").saveAsTable("joined_df")

    // Load the saved table
    val joinedDF = spark.table("joined_df")

    // Group by post details and calculate count and average
    joinedDF.groupBy(
        joinedDF("_Id"), joinedDF("_Title"), joinedDF("_Score")
      ).agg(
        count(joinedDF("_commentid")).alias("comment_count"),
        avg("_comment_score").alias("average_comment_score")
      ).write.mode("overwrite").saveAsTable("grouped_df")

    // Load the saved grouped table
    val groupedDF = spark.table("grouped_df")

    // Filter based on conditions and order by comment_count
    val result = groupedDF.filter(col("comment_count") > 10 && col("average_comment_score") > 5)
      .orderBy(col("comment_count").desc)

    // Show the result
    result.show()

    // Stop the Spark session
    spark.stop()
  }
}
