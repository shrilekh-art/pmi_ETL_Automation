from dotenv import load_dotenv
import os

# 🔥 Load environment variables
load_dotenv()

from pyspark.sql import SparkSession


def get_spark_session(app_name="ETL Framework"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


spark = get_spark_session()

df = spark.createDataFrame([(1, "A"), (2, "B"),(3,"C")], ["id", "name"])
df.show()