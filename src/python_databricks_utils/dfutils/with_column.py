import functools
import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StructType

from .fields import exists_field

logger = logging.getLogger(__name__)


# todo: clean me up!
def with_column(df: DataFrame, refs: list[str] | str, col: Column) -> DataFrame:
    if isinstance(refs, str):
        return with_column(df, refs.split("."), col)
    else:
        # foreach_field(df.schema, lambda _f, _refs: print(f"{_f=}, {_refs=}, {exists_field(df.schema, _refs)=}"))
        print(f"{refs=}")
        for _i in range(len(refs)):
            print(f"{_i=}")
            _ref = refs[:_i + 1]
            print(f"{_ref=}, {exists_field(df.schema, _ref)=}")

        def reduce(acc: Column, i: int) -> Column:
            print(f"reduce: {acc=}, {i=}")
            name = refs[i + 1]
            print(f"{name=}")
            parent = refs[:(i + 1)]
            print(f"{parent=}")
            exists = exists_field(df.schema, parent, data_type=StructType)
            print(f"{exists=}")
            if exists:
                r = F.col(".".join(parent)).withField(name, acc)
                print(f"{r=}")
                return r
            else:
                r = F.struct(acc).alias(refs[i])  # F.struct(acc).alias(refs[-(i+1)])
                print(f"{r=}")
                return r

        col = functools.reduce(lambda acc, i: reduce(acc, i), reversed(range(len(refs[1:]))), col.alias(refs[-1]))
        print(f"{col=}")

        print(f"{refs[0]=}")
        r = df.withColumn(refs[0], col)

        return r
