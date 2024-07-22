//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{col, countDistinct, avg}


object NonLazy1 {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder
      .appName("Non-Lazy performance test 1")
      .enableHiveSupport()
      .getOrCreate()

    // -- Get the number of posts and comments each user has made along with their average comment score
    
    val db = "stackoverflow"
    spark.sql(s"USE $db")
    
    val users = spark.table("users").withColumnRenamed("_creationDate", "_creationDateUser")
    val posts = spark.table("posts")
    val comments = spark.table("comments")

    users.join(
      posts.withColumnRenamed("_id", "_postId"), users("_Id") === posts("_OwnerUserId"), "left"
    ).write.mode("overwrite").saveAsTable("temp_user_posts")

    val userPosts = spark.table("temp_user_posts")

    users.join(
      comments.withColumnRenamed("_id", "_commentId"),
      users("_Id") === comments("_UserId"),
      "left"
    ).write.mode("overwrite").saveAsTable("temp_user_comments")

    val userComments = spark.table("temp_user_comments")

    userPosts.groupBy("_DisplayName").agg(
      countDistinct("_postId").alias("post_count")
    ).join(
      userComments.groupBy("_DisplayName").agg(
        countDistinct("_commentId").alias("comment_count"),
        avg("_Score").alias("average_comment_score")
      ),
      Seq("_DisplayName")
    ).write.mode("overwrite").saveAsTable("user_group_by")

    val userGroupBy = spark.table("user_group_by")

    // Perform the required aggregations
    val result = userGroupBy.orderBy(col("post_count").desc, col("comment_count").desc)

    result.show()

    result.explain(true)

    spark.stop()
  }
}
