from pyspark.sql.functions import col, when
from framework.spark_session import get_spark_session
from framework.logger import get_logger
import os
import pandas as pd

logger = get_logger()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

STAGING_PATH = os.path.join(BASE_DIR, "data", "staging", "policy")
CURATED_PATH = os.path.join(BASE_DIR, "data", "curated", "policy")


def process_curated_data():
    logger.info("Starting curated layer pipeline...")

    try:
        spark = get_spark_session()

        #  STEP 1 — READ USING PANDAS (NO HADOOP DEPENDENCY)
        input_file = os.path.join(STAGING_PATH, "policy_output.csv")

        logger.info(f"Reading staging file (Pandas): {input_file}")

        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            return

        pdf = pd.read_csv(input_file)

        #  STEP 2 — CONVERT TO SPARK DF
        df = spark.createDataFrame(pdf)

        logger.info(f"Initial Row Count: {df.count()}")

        df.show()
        df.printSchema()

        #  ================= BUSINESS LOGIC =================

        # 1. Commission Calculation (10%)
        df = df.withColumn("commission", col("premium") * 0.10)

        # 2. Policy Status Mapping
        df = df.withColumn(
            "policy_status",
            when(col("status") == "ACTIVE", "INFORCE")
            .when(col("status") == "LAPSED", "INACTIVE")
            .when(col("status") == "CANCELLED", "TERMINATED")
            .otherwise("UNKNOWN")
        )

        # 3. Premium Category
        df = df.withColumn(
            "premium_category",
            when(col("premium") < 15000, "LOW")
            .when((col("premium") >= 15000) & (col("premium") < 20000), "MEDIUM")
            .otherwise("HIGH")
        )

        logger.info("Business transformations applied")

        df.show()

        #  ================= WRITE OUTPUT =================

        os.makedirs(CURATED_PATH, exist_ok=True)

        output_file = os.path.join(CURATED_PATH, "policy_curated.csv")

        #  CONVERT BACK TO PANDAS (SAFE WRITE)
        df.toPandas().to_csv(output_file, index=False)

        logger.info(f"Curated data written to: {output_file}")

    except Exception as e:
        logger.error(f"Curated pipeline failed: {str(e)}")
        raise

    finally:
        logger.info("Curated pipeline completed")


if __name__ == "__main__":
    process_curated_data()