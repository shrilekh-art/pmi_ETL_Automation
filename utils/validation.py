def validate_row_count(source_count, target_count):
    assert source_count == target_count, (
        f"Row count mismatch: {source_count} != {target_count}"
    )


def validate_not_null(null_count, column):
    assert null_count == 0, (
        f"Null values found in column {column}"
    )