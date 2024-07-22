# Initialize Spark session
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from faker import Faker


spark: SparkSession = (
    SparkSession.builder.appName("FakeDataGenerator").enableHiveSupport().getOrCreate()
)

# Set the database
db = "stackoverflow"
spark.sql(f"USE {db}")

# Initialize Faker
fake = Faker()

# Define schemas for each table
badges_schema = StructType(
    [
        StructField("_Class", LongType(), True),
        StructField("_Date", TimestampType(), True),
        StructField("_Id", LongType(), True),
        StructField("_Name", StringType(), True),
        StructField("_TagBased", BooleanType(), True),
        StructField("_UserId", LongType(), True),
    ]
)

comments_schema = StructType(
    [
        StructField("_CreationDate", TimestampType(), True),
        StructField("_Id", LongType(), True),
        StructField("_PostId", LongType(), True),
        StructField("_Score", LongType(), True),
        StructField("_Text", StringType(), True),
        StructField("_UserDisplayName", StringType(), True),
        StructField("_UserId", LongType(), True),
    ]
)

posthistory_schema = StructType(
    [
        StructField("_Comment", StringType(), True),
        StructField("_ContentLicense", StringType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_Id", LongType(), True),
        StructField("_PostHistoryTypeId", LongType(), True),
        StructField("_PostId", LongType(), True),
        StructField("_RevisionGUID", StringType(), True),
        StructField("_Text", StringType(), True),
        StructField("_UserDisplayName", StringType(), True),
        StructField("_UserId", LongType(), True),
    ]
)

postlinks_schema = StructType(
    [
        StructField("_CreationDate", TimestampType(), True),
        StructField("_Id", LongType(), True),
        StructField("_LinkTypeId", LongType(), True),
        StructField("_PostId", LongType(), True),
        StructField("_RelatedPostId", LongType(), True),
    ]
)

posts_schema = StructType(
    [
        StructField("_AcceptedAnswerId", LongType(), True),
        StructField("_AnswerCount", LongType(), True),
        StructField("_Body", StringType(), True),
        StructField("_ClosedDate", TimestampType(), True),
        StructField("_CommentCount", LongType(), True),
        StructField("_CommunityOwnedDate", TimestampType(), True),
        StructField("_ContentLicense", StringType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_FavoriteCount", LongType(), True),
        StructField("_Id", LongType(), True),
        StructField("_LastActivityDate", TimestampType(), True),
        StructField("_LastEditDate", TimestampType(), True),
        StructField("_LastEditorDisplayName", StringType(), True),
        StructField("_LastEditorUserId", LongType(), True),
        StructField("_OwnerDisplayName", StringType(), True),
        StructField("_OwnerUserId", LongType(), True),
        StructField("_ParentId", LongType(), True),
        StructField("_PostTypeId", LongType(), True),
        StructField("_Score", LongType(), True),
        StructField("_Tags", StringType(), True),
        StructField("_Title", StringType(), True),
        StructField("_ViewCount", LongType(), True),
    ]
)

tags_schema = StructType(
    [
        StructField("_Count", LongType(), True),
        StructField("_ExcerptPostId", LongType(), True),
        StructField("_Id", LongType(), True),
        StructField("_TagName", StringType(), True),
        StructField("_WikiPostId", LongType(), True),
    ]
)

users_schema = StructType(
    [
        StructField("_AboutMe", StringType(), True),
        StructField("_AccountId", LongType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_DisplayName", StringType(), True),
        StructField("_DownVotes", LongType(), True),
        StructField("_Id", LongType(), True),
        StructField("_LastAccessDate", TimestampType(), True),
        StructField("_Location", StringType(), True),
        StructField("_Reputation", LongType(), True),
        StructField("_UpVotes", LongType(), True),
        StructField("_Views", LongType(), True),
        StructField("_WebsiteUrl", StringType(), True),
    ]
)

votes_schema = StructType(
    [
        StructField("_BountyAmount", LongType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_Id", LongType(), True),
        StructField("_PostId", LongType(), True),
        StructField("_UserId", LongType(), True),
        StructField("_VoteTypeId", LongType(), True),
    ]
)


# Function to generate fake data for each table
def generate_badges_data(num_records):
    return [
        (
            random.randint(1, 10),
            fake.date_time(),
            i,
            fake.word(),
            random.choice([True, False]),
            random.randint(1, 1000),
        )
        for i in range(1, num_records + 1)
    ]


def generate_comments_data(num_records):
    return [
        (
            fake.date_time(),
            i,
            random.randint(1, 1000),
            random.randint(0, 100),
            fake.text(),
            fake.name(),
            random.randint(1, 1000),
        )
        for i in range(1, num_records + 1)
    ]


def generate_posthistory_data(num_records):
    return [
        (
            fake.text(),
            fake.license_plate(),
            fake.date_time(),
            i,
            random.randint(1, 10),
            random.randint(1, 1000),
            fake.uuid4(),
            fake.text(),
            fake.name(),
            random.randint(1, 1000),
        )
        for i in range(1, num_records + 1)
    ]


def generate_postlinks_data(num_records):
    return [
        (
            fake.date_time(),
            i,
            random.randint(1, 10),
            random.randint(1, 1000),
            random.randint(1, 1000),
        )
        for i in range(1, num_records + 1)
    ]


def generate_posts_data(num_records):
    return [
        (
            random.randint(1, 1000),
            random.randint(0, 50),
            fake.text(),
            fake.date_time(),
            random.randint(0, 20),
            fake.date_time(),
            fake.license_plate(),
            fake.date_time(),
            random.randint(0, 10),
            i,
            fake.date_time(),
            fake.date_time(),
            fake.name(),
            random.randint(1, 1000),
            fake.name(),
            random.randint(1, 1000),
            random.randint(1, 1000),
            random.randint(1, 10),
            random.randint(0, 1000),
            fake.words(5),
            fake.sentence(),
            random.randint(1, 1000),
        )
        for i in range(1, num_records + 1)
    ]


def generate_tags_data(num_records):
    return [
        (
            random.randint(0, 1000),
            random.randint(1, 1000),
            i,
            fake.word(),
            random.randint(1, 1000),
        )
        for i in range(1, num_records + 1)
    ]


def generate_users_data(num_records):
    return [
        (
            fake.text(),
            random.randint(1, 1000),
            fake.date_time(),
            fake.name(),
            random.randint(0, 100),
            i,
            fake.date_time(),
            fake.city(),
            random.randint(1, 100000),
            random.randint(0, 100),
            random.randint(0, 1000),
            fake.url(),
        )
        for i in range(1, num_records + 1)
    ]


def generate_votes_data(num_records):
    return [
        (
            random.randint(0, 100),
            fake.date_time(),
            i,
            random.randint(1, 1000),
            random.randint(1, 1000),
            random.randint(1, 10),
        )
        for i in range(1, num_records + 1)
    ]


# Generate data for each table
NUM_OF_ROWS_INSERT = 1000 * 1000


badges_data = generate_badges_data(NUM_OF_ROWS_INSERT)
comments_data = generate_comments_data(NUM_OF_ROWS_INSERT)
posthistory_data = generate_posthistory_data(NUM_OF_ROWS_INSERT)
postlinks_data = generate_postlinks_data(NUM_OF_ROWS_INSERT)
posts_data = generate_posts_data(NUM_OF_ROWS_INSERT)
tags_data = generate_tags_data(NUM_OF_ROWS_INSERT)
users_data = generate_users_data(NUM_OF_ROWS_INSERT)
votes_data = generate_votes_data(NUM_OF_ROWS_INSERT)

# Create DataFrames
badges_df = spark.createDataFrame(badges_data, badges_schema)
comments_df = spark.createDataFrame(comments_data, comments_schema)
posthistory_df = spark.createDataFrame(posthistory_data, posthistory_schema)
postlinks_df = spark.createDataFrame(postlinks_data, postlinks_schema)
posts_df = spark.createDataFrame(posts_data, posts_schema)
tags_df = spark.createDataFrame(tags_data, tags_schema)
users_df = spark.createDataFrame(users_data, users_schema)
votes_df = spark.createDataFrame(votes_data, votes_schema)


# Save DataFrames to database
badges_df.write.insertInto("badges")
comments_df.write.insertInto("comments")
posthistory_df.write.insertInto("posthistory")
postlinks_df.write.insertInto("postlinks")
posts_df.write.insertInto("posts")
tags_df.write.insertInto("tags")
users_df.write.insertInto("users")
votes_df.write.insertInto("votes")

# Refresh tables
spark.sql("REFRESH TABLE badges")
spark.sql("REFRESH TABLE comments")
spark.sql("REFRESH TABLE posthistory")
spark.sql("REFRESH TABLE postlinks")
spark.sql("REFRESH TABLE posts")
spark.sql("REFRESH TABLE tags")
spark.sql("REFRESH TABLE users")
spark.sql("REFRESH TABLE votes")

# Stop SparkSession
spark.stop()
