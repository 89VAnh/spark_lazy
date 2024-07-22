//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, avg}

object Lazy1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Lazy performance test 1")
      .enableHiveSupport()
      .getOrCreate()
    
    // -- Get the number of posts and comments each user has made along with their average comment score

    val db = "stackoverflow"
    spark.sql(s"USE $db")
    
    val users = spark.table("users")
    val posts = spark.table("posts")
    val comments = spark.table("comments")

    // Left join posts with users
    val userPosts = users.join(posts, users("_Id") === posts("_OwnerUserId"), "left")

    // Left join comments with users
    val userComments = users.join(comments, users("_Id") === comments("_UserId"), "left")

    val userGroupBy = userPosts.groupBy("_DisplayName")
    .agg(countDistinct("posts._Id").as("post_count"))
    .join(
        userComments.groupBy("_DisplayName")
        .agg(
            countDistinct("comments._Id").as("comment_count"),
            avg("comments._Score").as("average_comment_score")
        ),
        Seq("_DisplayName")
    )

    // Perform the required aggregations
    val result = userGroupBy.orderBy(col("post_count").desc, col("comment_count").desc)

    // Show the result
    result.show()

    result.explain(true)

    spark.stop()
  }
}
