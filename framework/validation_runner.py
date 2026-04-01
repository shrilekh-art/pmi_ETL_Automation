import json
import os
import pandas as pd
from datetime import datetime

from framework.spark_session import get_spark_session
from framework.logger import get_logger
from utils.validation import *
from utils.file_utils import get_latest_csv_file
from pyspark.sql.functions import col, isnan

logger = get_logger()
spark = get_spark_session()


def load_df(path):
    pdf = pd.read_csv(path)
    return spark.createDataFrame(pdf)


def run_validations(config_path):
    logger.info(f"Starting config-driven validation using: {config_path}")

    with open(config_path) as f:
        config = json.load(f)

    results = []

    try:
        raw_file = get_latest_csv_file(config["paths"]["raw"])
        staging_file = get_latest_csv_file(config["paths"]["staging"])

        logger.info(f"Raw file: {raw_file}")
        logger.info(f"Staging file: {staging_file}")

        src = load_df(raw_file)
        tgt = load_df(staging_file)

        for rule in config["validations"]:
            validation_name = rule["type"]

            try:
                logger.info(f"Running validation: {validation_name}")

                #  ROW COUNT
                if validation_name == "row_count":
                    raw_count = src.count()

                    null_filtered = src.filter(
                        col("premium").isNull() | isnan(col("premium"))
                    ).count()

                    expected = raw_count - null_filtered
                    actual = tgt.count()

                    assert actual == expected, f"{expected} != {actual}"

                # NULL CHECK
                elif validation_name == "null_check":
                    for column in rule["columns"]:
                        validate_no_nulls(tgt, column)

                # SCHEMA
                elif validation_name == "schema":
                    validate_schema(tgt, rule["expected_columns"])

                else:
                    raise ValueError(f"Unknown validation: {validation_name}")

                results.append({
                    "validation": validation_name,
                    "status": "PASS",
                    "timestamp": str(datetime.now())
                })

            except Exception as e:
                logger.error(f"{validation_name} failed: {str(e)}")

                results.append({
                    "validation": validation_name,
                    "status": "FAIL",
                    "error": str(e),
                    "timestamp": str(datetime.now())
                })

                raise  # stop execution

        # SAVE REPORT
        save_report(results)

        logger.info("All validations passed")

    except Exception as e:
        save_report(results)
        logger.error("Validation execution failed")
        raise


def save_report(results):
    os.makedirs("reports", exist_ok=True)

    report_file = "reports/validation_report.json"

    with open(report_file, "w") as f:
        json.dump(results, f, indent=4)

    logger.info(f"Validation report generated: {report_file}")