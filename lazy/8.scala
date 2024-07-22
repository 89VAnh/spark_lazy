//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// -- Retrieve the details of posts with more than 10 comments and an average comment score greater than 5

object Lazy8 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Lazy performance test 8")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the tables into DataFrames
    val postsDF = spark.table("posts")
    val commentsDF = spark.table("comments")

    // Join the posts and comments DataFrames
    val joinedDF = postsDF.join(commentsDF, postsDF("_Id") === commentsDF("_PostId"))

    // Group by post details and filter based on the conditions
    val result = joinedDF.groupBy(
        postsDF("_Id"), postsDF("_Title"), postsDF("_Score")
      ).agg(
        count(commentsDF("_Id")).alias("comment_count"),
        avg(commentsDF("_Score")).alias("average_comment_score")
      ).filter(col("comment_count") > 10 && col("average_comment_score") > 5)
      .orderBy(col("comment_count").desc)

    // Show the result
    result.show()

    // Stop the Spark session
    spark.stop()
  }
}
