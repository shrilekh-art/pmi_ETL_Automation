from pyspark.sql.functions import col
from framework.spark_session import get_spark_session
from framework.logger import get_logger
import os

# ✅ LOGGER INITIALIZATION (TOP LEVEL)
logger = get_logger()

# ✅ PROJECT ROOT PATH (ALREADY CORRECT)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAW_PATH = os.path.join(BASE_DIR, "data", "raw")
STAGING_PATH = os.path.join(BASE_DIR, "data", "staging", "policy")


def get_latest_file_path(base_path):
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.startswith("policy") and file.endswith(".csv"):
                return os.path.join(root, file)
    return None


def process_policy_data():

    # ✅ STEP 1 — START LOG (VERY IMPORTANT)
    logger.info("Starting staging pipeline...")

    spark = get_spark_session()

    # ✅ STEP 2 — DEBUG LOGS (ADDED HERE 🔥)
    logger.info(f"RAW PATH: {RAW_PATH}")
    logger.info(f"STAGING PATH: {STAGING_PATH}")

    file_path = get_latest_file_path(RAW_PATH)

    # ✅ STEP 3 — FILE FOUND DEBUG (ADDED HERE 🔥)
    logger.info(f"FILE FOUND: {file_path}")

    if not file_path:
        logger.error("No policy file found in raw layer")
        return

    logger.info(f"Reading file: {file_path}")

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # ✅ STEP 4 — VERIFY DATA READ (ADDED HERE 🔥)
    logger.info("Showing sample data:")
    df.show()

    logger.info("Printing schema:")
    df.printSchema()

    logger.info("Applying transformations...")

    # 🔹 Type Casting
    df = df.withColumn("premium", col("premium").cast("double"))

    # 🔹 Null Handling
    before_null = df.count()
    df = df.dropna(subset=["policy_id", "customer_id", "premium"])
    after_null = df.count()

    logger.info(f"Rows before null removal: {before_null}")
    logger.info(f"Rows after null removal: {after_null}")

    # 🔹 Deduplication
    before_dedup = df.count()
    df = df.dropDuplicates(["policy_id"])
    after_dedup = df.count()

    logger.info(f"Rows before dedup: {before_dedup}")
    logger.info(f"Rows after dedup: {after_dedup}")

    # ✅ STEP 5 — WRITE OUTPUT (UPDATED)
    logger.info(f"Writing staging data to: {STAGING_PATH}")

    output_file = os.path.join(STAGING_PATH, "policy_output.csv")

    df.toPandas().to_csv(output_file, index=False)

    logger.info(f"Data written using Pandas to: {output_file}")

    logger.info("Data written successfully")

    # ✅ FINAL LOG
    logger.info("Staging pipeline completed successfully")


if __name__ == "__main__":
    process_policy_data()