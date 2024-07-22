//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object NonLazy3 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Non-Lazy performance test 3")
      .enableHiveSupport()
      .getOrCreate()

    // -- Finding posts with the highest score

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the users and posts tables
    val users = spark.table("users")
    val posts = spark.table("posts")

    // Find the maximum score
    posts.select(max("_Score").alias("max_score")).write.mode("overwrite").saveAsTable("max_score")

    val maxScoreDF = spark.table("max_score")

    // Filter the posts with the maximum score by joining
    val result = posts.join(maxScoreDF, posts("_Score") === maxScoreDF("max_score"))
      .select(posts("_Id"), posts("_Title"), posts("_Score"))

    // Show the result
    result.show()

    result.explain(true)

    spark.stop()
  }
}
