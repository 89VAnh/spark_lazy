from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from readHdfsFiles import get_hdfs_files

spark: SparkSession = (
    SparkSession.builder.appName("Import stackoverflow")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
    .enableHiveSupport()
    .getOrCreate()
)

database = "stackoverflow_test"

spark.sql("use " + database)

folder_name = "/datasets/stackoverflow"


def get_tables_name() -> List[str]:

    return get_hdfs_files(folder_name)


file_names: List[str] = get_tables_name()

for file_name in file_names:
    file_name = file_name.lower()
    path_file = f"{folder_name}/{file_name}"

    df = spark.read.parquet(path_file)

    df.write.mode("overwrite").saveAsTable(file_name)

    print(f"Import table {file_name} done!")

spark.stop()
