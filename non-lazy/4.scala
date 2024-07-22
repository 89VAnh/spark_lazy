//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-core:3.5.1"
//> using dep "org.apache.spark::spark-sql:3.5.1"

import org.apache.spark.sql.SparkSession

object NonLazy4 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Non-lazy performance test 4")
      .enableHiveSupport()
      .getOrCreate()

    // -- Get the detailed post history including user details

    // Set the database to use
    val db = "stackoverflow"
    spark.sql(s"USE $db")

    // Load the posthistory and users tables
    val posthistory = spark.table("posthistory")
    val users = spark.table("users")

    // Join posthistory and users on user ID
    posthistory.join(users, posthistory("_UserId") === users("_Id"))
      .select(
        posthistory("_Id"),
        posthistory("_PostId"),
        posthistory("_PostHistoryTypeId"),
        posthistory("_CreationDate"),
        posthistory("_UserDisplayName"),
        posthistory("_Text"),
        users("_DisplayName")
      )
      .write.mode("overwrite").saveAsTable("posthistory_users")

    val posthistoryUsers = spark.table("posthistory_users")

    val result = posthistoryUsers.orderBy("_CreationDate")

    // Show the result
    result.show()

    // Optionally, explain the query plan
    result.explain(true)

    // Stop the Spark session
    spark.stop()
  }
}
