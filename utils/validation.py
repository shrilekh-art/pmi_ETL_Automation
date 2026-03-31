def validate_row_count(source_count, target_count):
    if source_count != target_count:
        raise AssertionError(
            f"Row count mismatch: {source_count} != {target_count}"
        )


def validate_not_null(null_count, column):
    if null_count != 0:
        raise AssertionError(
            f"Null values found in column {column}"
        )