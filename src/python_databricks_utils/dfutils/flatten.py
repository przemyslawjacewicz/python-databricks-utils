import functools
from typing import Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.types import StructField, ArrayType, MapType, StructType

from python_spark_utils.utils.utils import flatten_list


def flatten_schema(spark: SparkSession, schema: StructType, sep: str) -> StructType:
    return flatten_dataframe(spark.createDataFrame([], schema), sep).schema


def flatten_dataframe(df: DataFrame, sep: str) -> DataFrame:
    def flatten_field(field: StructField, parent_refs: list[str]) -> list[Tuple[Column, list[str]]]:
        refs = parent_refs + [field.name]
        ref = ".".join([f"`{_r}`" for _r in refs])

        def get_field(col: Column, refs: list[str]) -> Column:
            return functools.reduce(lambda acc, r: acc.getField(r), refs, col)

        if isinstance(field.dataType, MapType):
            key_type = field.dataType.keyType
            if isinstance(key_type, StructType):
                key_fields = key_type.fields
                key_fields_flat = flatten_list([flatten_field(_f, []) for _f in key_fields])
                key_flat_fn = lambda key_col: F.transform_keys(
                    key_col,
                    lambda k, _: F.struct([get_field(k, _r).alias(sep.join(_r)) for (_, _r) in key_fields_flat])
                )
            else:
                key_flat_fn = lambda key_col: key_col

            value_type = field.dataType.valueType
            if isinstance(value_type, StructType):
                value_fields = value_type.fields
                value_fields_flat = flatten_list([flatten_field(_f, []) for _f in value_fields])
                value_flat_fn = lambda value_col: F.transform_values(
                    value_col,
                    lambda _, v: F.struct([get_field(v, _r).alias(sep.join(_r)) for (_, _r) in value_fields_flat])
                )
            else:
                value_flat_fn = lambda value_col: value_col

            return [(key_flat_fn(value_flat_fn(F.col(ref))), refs)]
        elif isinstance(field.dataType, ArrayType):
            element_type = field.dataType.elementType
            field.dataType.containsNull
            if isinstance(element_type, StructType):
                element_fields = element_type.fields
                element_fields_flat = flatten_list([flatten_field(_f, []) for _f in element_fields])
                element_flat_fn = lambda element_col: F.transform(
                    element_col,
                    lambda e: F.struct([get_field(e, _r).alias(sep.join(_r)) for (_, _r) in element_fields_flat])
                )
            else:
                element_flat_fn = lambda element_col: element_col

            return [(element_flat_fn(F.col(ref)), refs)]
        elif isinstance(field.dataType, StructType):
            return flatten_list([flatten_field(_f, refs) for _f in field.dataType.fields])
        else:
            return [(F.col(ref), refs)]

    flat = flatten_list([flatten_field(_f, []) for _f in df.schema.fields])
    s = [_col.alias(sep.join(_refs)) for (_col, _refs) in flat]

    return df.select(*s)
