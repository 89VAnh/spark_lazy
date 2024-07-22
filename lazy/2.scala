//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Lazy2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Lazy performance test 2")
      .enableHiveSupport()
      .getOrCreate()

    // -- Count the number of posts for each user

    // Set the database
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the users and posts tables
    val users = spark.table("users")
    val posts = spark.table("posts")

    // Find users with more than 10 posts
    val userIdsWithMoreThan10Posts = posts.groupBy("_OwnerUserId")
      .count()
      .filter(col("count") > 10)
      .select("_OwnerUserId")

    // Join the result with users to get the required columns
    val result = users.join(
      userIdsWithMoreThan10Posts,
      users("_Id") === userIdsWithMoreThan10Posts("_OwnerUserId")
    ).select(users("_Id"), users("_DisplayName"))

    // Show the result
    result.show()

    result.explain(true)

    spark.stop()
  }
}
