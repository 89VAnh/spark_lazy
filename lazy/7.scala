//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// -- Find the most linked posts in the post links table
object Lazy7 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Lazy performance test 7")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the table into a DataFrame
    val df = spark.table("postlinks")

    // Perform the query to find the most linked posts
    val result = df.groupBy("_PostId")
      .agg(count("*").alias("link_count"))
      .orderBy(col("link_count").desc)

    // Show the result
    result.show()

    // Explain the query plan
    result.explain(true)

    // Stop the Spark session
    spark.stop()
  }
}
