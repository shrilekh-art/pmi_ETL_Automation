import pandas as pd
from framework.spark_session import get_spark_session
from utils.validation import *

spark = get_spark_session()

def load_df(path):
    pdf = pd.read_csv(path)
    return spark.createDataFrame(pdf)


def test_curated_schema():
    df = load_df("data/curated/policy/policy_curated.csv")

    expected_columns = [
        "policy_id",
        "customer_id",
        "premium",
        "status",
        "issue_date",
        "commission",
        "policy_status",
        "premium_category"
    ]

    validate_schema(df, expected_columns)


def test_commission_logic():
    df = load_df("data/curated/policy/policy_curated.csv")

    validate_commission(df)


def test_policy_status_values():
    df = load_df("data/curated/policy/policy_curated.csv")

    allowed = ["INFORCE", "INACTIVE", "TERMINATED"]

    validate_column_values(df, "policy_status", allowed)