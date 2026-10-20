import logging

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

def is_count_equal(left: DataFrame, right: DataFrame) -> bool:
    return left.count() == right.count()

# todo: add tests
# todo: sorted ?
def is_equal_schema(left: StructType, right: StructType) -> bool:
    pass

def is_schema_equal(
        left: StructType,
        right: StructType,
        ignore_case: bool = False,
        ignore_nullable: bool = True,
        ignore_metadata: bool = True,
) -> bool:
    def normalize_data_type(data_type):
        if isinstance(data_type, StructType):
            return {
                "type": "struct",
                "fields": [
                    normalize_field(field)
                    for field in data_type.fields
                ],
            }

        if hasattr(data_type, "elementType"):
            result = {
                "type": data_type.typeName(),
                "elementType": normalize_data_type(data_type.elementType),
            }
            if not ignore_nullable and hasattr(data_type, "containsNull"):
                result["containsNull"] = data_type.containsNull
            return result

        if hasattr(data_type, "keyType") and hasattr(data_type, "valueType"):
            result = {
                "type": data_type.typeName(),
                "keyType": normalize_data_type(data_type.keyType),
                "valueType": normalize_data_type(data_type.valueType),
            }
            if not ignore_nullable and hasattr(data_type, "valueContainsNull"):
                result["valueContainsNull"] = data_type.valueContainsNull
            return result

        return data_type.jsonValue()

    def normalize_field(field):
        result = {
            "name": field.name.lower() if ignore_case else field.name,
            "type": normalize_data_type(field.dataType),
        }

        if not ignore_nullable:
            result["nullable"] = field.nullable

        if not ignore_metadata:
            result["metadata"] = field.metadata

        return result

    def normalize_schema(schema: StructType):
        return {
            "type": "struct",
            "fields": [
                normalize_field(field)
                for field in schema.fields
            ]
        }

    return normalize_schema(left) == normalize_schema(right)


def are_schemas_equal(schema1: StructType, schema2: StructType) -> bool:
    def normalize(schema: StructType) -> StructType:
        return StructType([StructField(f.name.upper(), f.dataType) for f in schema.fields])

    return set(normalize(schema1).fields) == set(normalize(schema2).fields)


# todo: add tests
# todo: add schema check as first step: check schema size -> check sorted schema column matching
def is_equal_dataframe(left: DataFrame, right: DataFrame) -> bool:
    left_count = left.count()
    right_count = right.count()
    if left_count != right_count:
        logger.debug(f"count mismatch: {left_count=}, {right_count=}.")
        return False
    logger.debug(f"count match: {left_count=}, {right_count=}.")

    left_diff_right = left.exceptAll(right)
    if not left_diff_right.isEmpty():
        logger.debug(f"left diff right row-level mismatch: {left_diff_right.count()=}.")
        return False
    logger.debug("left diff right row-level match.")

    right_diff_left = right.exceptAll(left)
    if not right_diff_left.isEmpty():
        logger.debug(f"right diff left row-level mismatch: {right_diff_left.count()=}.")
        return False
    logger.debug("right diff left row-level match.")

    return True

def is_content_equal(left: DataFrame, right: DataFrame) -> bool:
    left_diff_right = left.exceptAll(right)
    if not left_diff_right.isEmpty():
        logger.info(f"left diff right row-level mismatch: {left_diff_right.count()=}.")
        return False
    logger.info(f"left diff right row-level match.")

    right_diff_left = right.exceptAll(left)
    if not right_diff_left.isEmpty():
        logger.info(f"right diff left row-level mismatch: {right_diff_left.count()=}.")
        return False
    logger.info(f"right diff left row-level match.")

    return True



def are_dataframes_equal(df1: DataFrame, df2: DataFrame) -> bool:
    """Multi-stage approach for comparing very large DataFrames"""

    # 1. Check schema
    if not are_schemas_equal(df1.schema, df2.schema):
        print("Schemas don't match!")
        return False

    # 2. Check row counts
    if df1.count() != df2.count():
        print(f"Row counts don't match!")
        return False

    # 3. Check data distribution stats for fast-fail
    for c in sorted(df1.columns):
        if df1.select(c).distinct().count() != df2.select(c).distinct().count():
            print(f"Column {c} has different number of distinct values")
            return False

    # 4. If all above tests pass, do the full hash comparison
    return hashdiff(df1, df2).isEmpty()
