from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
load_dotenv()

def get_spark_session(app_name="ETL Framework"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )