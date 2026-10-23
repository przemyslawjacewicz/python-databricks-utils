import logging

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def is_equal_schema(
        left: StructType,
        right: StructType,
        ignore_case: bool = True,
        ignore_nullable: bool = True,
        ignore_metadata: bool = True,
) -> bool:
    def normalize_data_type(data_type):
        if isinstance(data_type, StructType):
            return {
                "type": "struct",
                "fields": [normalize_field(field) for field in data_type.fields],
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
            "fields": [normalize_field(field) for field in schema.fields]
        }

    return normalize_schema(left) == normalize_schema(right)


def is_equal_dataframe(
        left: DataFrame,
        right: DataFrame,
        ignore_case: bool = True,
        ignore_nullable: bool = True,
        ignore_metadata: bool = True
) -> bool:
    # count
    if (left_count := left.count()) != (right_count := right.count()):
        logger.debug(f"count mismatch: {left_count=} != {right_count=}")
        return False

    # schema
    if not is_equal_schema(left.schema, right.schema):
        logger.debug(f"schema mismatch: {left.schema=} != {right.schema=}")
        return False

    # data distribution
    for c in sorted(left.columns):
        if (left_c_count := left.select(c).distinct().count()) != (right_c_count := right.select(c).distinct().count()):
            print(f"distinct values count mismatch for column {c}: {left_c_count=} != {right_c_count=}")
            return False

    # left diff right
    if not (left_diff_right := left.exceptAll(right)).isEmpty():
        logger.debug(f"left diff right row-level mismatch: {left_diff_right.count()=}")
        return False

    # right diff left
    if not (right_diff_left := right.exceptAll(left)).isEmpty():
        logger.debug(f"right diff left row-level mismatch: {right_diff_left.count()=}")
        return False

    return True
