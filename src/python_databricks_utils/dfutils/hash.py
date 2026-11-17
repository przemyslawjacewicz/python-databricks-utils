# todo: check how hashing behaves when we have all null values
# todo: compare hash for two dataframes where the second one's schema extends the first one's but all cols are nulls
# for hashing check how this behaves when dataframes have different schema with null values in extra columns
import logging
from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

logger = logging.getLogger()


def hash_col(cols: list, chunk_size: int = 100, hash_fn: Callable[..., Column] = F.xxhash64) -> Column:
    cols_sorted = sorted(cols)
    cols_chunk_hashed = [hash_fn(*cols_sorted[i:i + chunk_size]) for i in range(0, len(cols_sorted), chunk_size)]
    return hash_fn(*cols_chunk_hashed) if len(cols_chunk_hashed) > 1 else cols_chunk_hashed[0]


def hash_dataframe(
        df: DataFrame,
        hash_fn: Callable[..., Column] = F.xxhash64,
        chunk_size: int = 100,
        buckets: int = 8
) -> DataFrame:
    logger.info(f"{len(df.schema)=}, {chunk_size=}, {buckets=}")

    return (
        df
        .select(hash_col(df.columns, chunk_size, hash_fn).alias("_row_hash"))
        .withColumn("_bucket", F.pmod(F.col("_row_hash"), F.lit(buckets)))
        .groupBy("_bucket", "_row_hash")
        .agg(F.count(F.lit(1)).alias("_row_count"))
    )
