//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// -- Top 10 users with the highest reputation and average post score

object Lazy6 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Lazy performance test 6")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the users and posts tables
    val users = spark.table("users")
    val posts = spark.table("posts")

    // Perform the join and the aggregations
    val topUsers = users.join(posts, users("_Id") === posts("_OwnerUserId"), "left")
      .groupBy(users("_DisplayName"), users("_Reputation"))
      .agg(
        count(posts("_Id")).alias("post_count"),
        avg(posts("_Score")).alias("average_post_score")
      )
      .orderBy(col("_Reputation").desc)
      .limit(10)

    // Show the result
    topUsers.show()

    // Explain the query plan
    topUsers.explain(true)

    // Stop the Spark session
    spark.stop()
  }
}
