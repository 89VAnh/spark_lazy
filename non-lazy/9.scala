//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
// -- Get the average score of comments for each user
object NonLazy9 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Non-lazy performance test 9")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the posts table into a DataFrame
    val postsDF = spark.table("posts")

    // Perform the aggregation and sorting
    postsDF
      .groupBy(col("_Tags"))
      .agg(count("*").alias("tag_count"))
      .write.mode("overwrite").saveAsTable("post_group")

    val post_group = spark.table("post_group")
    
    val result = post_group.orderBy(col("tag_count").desc)

    // Show the result
    result.show()

    // Optionally, explain the query plan
    result.explain(true)

    // Stop the Spark session
    spark.stop()
  }
}
