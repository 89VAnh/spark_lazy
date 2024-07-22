//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// -- Find the most linked posts in the post links table
object NonLazy7 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Non-Lazy performance test 7")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the table into a DataFrame
    val df = spark.table("postlinks")

    // Group by _PostId and count the links, save the result
    df.groupBy("_PostId")
      .agg(count("*").alias("link_count"))
      .write.mode("overwrite").saveAsTable("postlinks_grouped")

    // Load the saved table
    val postlinksGrouped = spark.table("postlinks_grouped")

    // Perform the query to order by link_count descending
    val result = postlinksGrouped.orderBy(col("link_count").desc)

    // Show the result
    result.show()

    // Explain the query plan
    result.explain(true)

    // Stop the Spark session
    spark.stop()
  }
}
