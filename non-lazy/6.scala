//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NonLazy6 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Non-Lazy performance test 6")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the users and posts tables
    val users = spark.table("users")
    val posts = spark.table("posts").drop("_creationdate")

    // Join users with posts
    users.join(
      posts.select(col("_Id").alias("post_id"), col("_OwnerUserId"), col("_Score")),
      users("_Id") === posts("_OwnerUserId"),
      "left"
    ).write.mode("overwrite").saveAsTable("users_posts")

    // Load the aggregated table
    val usersPostsAgg = spark.table("users_posts")
      .groupBy(col("_DisplayName"), col("_Reputation"))
      .agg(
        count("post_id").alias("post_count"),
        avg("_Score").alias("average_post_score")
      )

    // Save the aggregated table
    usersPostsAgg.write.mode("overwrite").saveAsTable("users_posts_agg")

    // Load and sort the aggregated table
    val usersPostsAggSorted = spark.table("users_posts_agg")
      .orderBy(col("_Reputation").desc)

    // Save the sorted aggregated table
    usersPostsAggSorted.write.mode("overwrite").saveAsTable("users_posts_agg_sorted")

    // Load the top 10 users
    val result = spark.table("users_posts_agg_sorted").limit(10)

    // Show the result
    result.show()

    // Explain the query plan
    result.explain(true)

    // Stop the Spark session
    spark.stop()
  }
}
