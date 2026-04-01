from pyspark.sql.functions import col

#  1. ROW COUNT VALIDATION
def validate_row_count(src_df, tgt_df):
    src_count = src_df.count()
    tgt_count = tgt_df.count()

    assert src_count == tgt_count, f"Row count mismatch: {src_count} != {tgt_count}"


#  2. NULL CHECK
def validate_no_nulls(df, column):
    null_count = df.filter(col(column).isNull()).count()

    assert null_count == 0, f"Null values found in column {column}: {null_count}"


#  3. SCHEMA VALIDATION
def validate_schema(df, expected_columns):
    actual_columns = df.columns

    assert actual_columns == expected_columns, f"Schema mismatch: {actual_columns} != {expected_columns}"


#  4. BUSINESS RULE VALIDATION
def validate_commission(df):
    invalid = df.filter(col("commission") != col("premium") * 0.10).count()

    assert invalid == 0, f"Invalid commission records: {invalid}"


# 5. VALUE CHECK
def validate_column_values(df, column, allowed_values):
    invalid = df.filter(~col(column).isin(allowed_values)).count()

    assert invalid == 0, f"Invalid values in {column}: {invalid}"