from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

def get_spark_session(app_name="ETL Framework"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )