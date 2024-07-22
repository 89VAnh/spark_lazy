//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NonLazy5 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder
      .appName("Non-Lazy performance test 5")
      .enableHiveSupport()
      .getOrCreate()

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the badges table
    val badges = spark.table("badges")

    // Find the number of badges awarded each day and save the result to a table
    badges.groupBy("_Date").agg(count("*").alias("badge_count"))
      .write.mode("overwrite").saveAsTable("badges_count")

    // Load the saved table
    val badgesCount = spark.table("badges_count")

    // Order the results by date
    val badgesPerDay = badgesCount.orderBy(col("_Date"))

    // Show the result
    badgesPerDay.show()

    // Explain the query plan
    badgesPerDay.explain(true)

    // Stop the Spark session
    spark.stop()
  }
}
