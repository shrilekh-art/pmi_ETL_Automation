from pyspark.sql.functions import col, when
from framework.spark_session import get_spark_session
from framework.logger import get_logger
from framework.error_handler import handle_error
import os
import pandas as pd

logger = get_logger()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

STAGING_PATH = os.path.join(BASE_DIR, "data", "staging", "policy")
CURATED_PATH = os.path.join(BASE_DIR, "data", "curated", "policy")


def process_curated_data():

    try:
        logger.info("Starting curated pipeline")

        spark = get_spark_session()

        # READ USING PANDAS
        input_file = os.path.join(STAGING_PATH, "policy_output.csv")

        logger.info(f"Reading staging file: {input_file}")

        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")

        pdf = pd.read_csv(input_file)

        df = spark.createDataFrame(pdf)

        logger.info(f"Initial Row Count: {df.count()}")

        df.show()
        df.printSchema()

        # BUSINESS LOGIC

        # Commission
        df = df.withColumn("commission", col("premium") * 0.10)

        # Status Mapping
        df = df.withColumn(
            "policy_status",
            when(col("status") == "ACTIVE", "INFORCE")
            .when(col("status") == "LAPSED", "INACTIVE")
            .when(col("status") == "CANCELLED", "TERMINATED")
            .otherwise("UNKNOWN")
        )

        # Premium Category
        df = df.withColumn(
            "premium_category",
            when(col("premium") < 15000, "LOW")
            .when((col("premium") >= 15000) & (col("premium") < 20000), "MEDIUM")
            .otherwise("HIGH")
        )

        logger.info("Business transformations applied")

        df.show()

        # WRITE OUTPUT
        os.makedirs(CURATED_PATH, exist_ok=True)

        output_file = os.path.join(CURATED_PATH, "policy_curated.csv")

        df.toPandas().to_csv(output_file, index=False)

        logger.info(f"Curated data written to: {output_file}")

        logger.info("Curated pipeline completed successfully")

    except Exception as e:
        handle_error(e, "curated layer")
        raise


if __name__ == "__main__":
    process_curated_data()