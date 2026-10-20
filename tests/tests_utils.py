from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


# ignoreNullable
# ignoreColumnOrder
def assert_schema_equal(
        actual: StructType,
        expected: StructType,
        ignore_nullable: bool = False,
        ignore_column_order: bool = False
):
    actual_to_compare = actual.toNullable() if ignore_nullable else actual
    print(f"{actual_to_compare=}")
    expected_to_compare = expected.toNullable() if ignore_nullable else expected
    print(f"{expected_to_compare=}")

    assert actual_to_compare == expected_to_compare, f"{actual} != {expected}"

# todo: assert is_equal(actual, expected) ?
def assert_dataframe_equal(
        actual: DataFrame,
        expected: DataFrame,
        ignore_nullable: bool = False,
        ignore_column_order: bool = False
):
    assert_schema_equal(actual.schema, expected.schema, ignore_nullable, ignore_column_order)
    assert actual.collect() == expected.collect(), f"{actual.collect()} != {expected.collect()}"
