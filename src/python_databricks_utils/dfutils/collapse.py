import functools

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from python_spark_utils.utils.utils import flatten_list
from .fields import foreach_field
from .with_column import with_column


# todo: add tests
def collapse_schema(schema: StructType, sep: str) -> StructType:
    pass


# todo: clean me up
# todo: add tests
def collapse_dataframe(df: DataFrame, sep: str) -> DataFrame:
    refs = []
    foreach_field(df.schema, lambda _f, _refs: refs.append(flatten_list([__ref.split(sep) for __ref in _refs])))
    print(f"{refs=}")

    c = "c"  # todo: make it random

    z = df.withColumn(c, F.struct(F.lit("dummy").alias("dummy")))
    r = functools.reduce(lambda acc, ref: with_column(acc, [c] + ref, F.col(sep.join(ref))), refs, z)
    r.printSchema()
    r.show()

    return r.select(f"{c}.*").drop("dummy")
