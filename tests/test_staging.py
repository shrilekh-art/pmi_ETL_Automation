import pandas as pd
from framework.spark_session import get_spark_session
from utils.validation import *
from utils.file_utils import get_latest_csv_file
from pyspark.sql.functions import col,isnan

spark = get_spark_session()


def load_df(path):
    pdf = pd.read_csv(path)
    return spark.createDataFrame(pdf)


#  UNIVERSAL ROW COUNT TEST
def test_staging_row_count():
    raw_file = get_latest_csv_file("data/raw")
    staging_file = get_latest_csv_file("data/staging/policy")

    src = load_df(raw_file)
    tgt = load_df(staging_file)

    raw_count = src.count()
    staging_count = tgt.count()

    # Count rows removed due to null premium

    null_filtered = src.filter(
        col("premium").isNull() | isnan(col("premium"))
    ).count()
    src.select("premium").show()
    expected_count = raw_count - null_filtered

    assert staging_count == expected_count, \
        f"Expected {expected_count} rows after null removal, got {staging_count}"

#  UNIVERSAL NULL CHECK
def test_staging_no_nulls():
    staging_file = get_latest_csv_file("data/staging/policy")

    assert staging_file is not None, "No file found in staging layer"

    df = load_df(staging_file)

    validate_no_nulls(df, "policy_id")
    validate_no_nulls(df, "customer_id")
    validate_no_nulls(df, "premium")