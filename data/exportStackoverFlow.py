from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from readHdfsFiles import get_hdfs_files

spark: SparkSession = (
    SparkSession.builder.appName("Export stackoverflow")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
    .enableHiveSupport()
    .getOrCreate()
)

database = "stackoverflow"

spark.sql("use " + database)


def get_tables_name() -> List[str]:
    folder_name = "/datasets/es.stackoverflow.com"

    return get_hdfs_files(folder_name)


folder_name = "/datasets/stackoverflow"

file_names: List[str] = get_tables_name()

for file_name in file_names:
    file_name = file_name.lower()
    path_file = f"{folder_name}/{file_name}"

    df = spark.table(file_name)

    df.write.mode("overwrite").parquet(path_file)

    print(f"Export table {file_name} done!")
spark.stop()
