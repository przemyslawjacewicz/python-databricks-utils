# todo: check how fingerprinting behaves when we have all null values
# todo: compare fingerprints for two dataframes where the second one's schema extends the first one's but all cols are nulls
# for fingerprinting check how this behaves when dataframes have different schema with null values in extra columns

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column


# todo: add tests
def fingerprint_col(cols: list, chunk_size: int = 100) -> Column:
    cols_sorted = sorted(cols)
    cols_chunk_hashed = [F.xxhash64(*cols_sorted[i:i + chunk_size]) for i in range(0, len(cols_sorted), chunk_size)]
    return F.xxhash64(*cols_chunk_hashed) if len(cols_chunk_hashed) > 1 else cols_chunk_hashed[0]

def fingerprint_col(
        cols: list[Column] | list[str],
        hash_fn: Callable[..., Column] = F.xxhash64,
        chunk_size: int = 100
) -> Column:
    cols_sorted = sorted(cols)
    row_hashes = [hash_fn(*cols_sorted[i:i + chunk_size]) for i in range(0, len(cols_sorted), chunk_size)]
    return hash_fn(*row_hashes) if len(row_hashes) > 1 else row_hashes[0]

# todo: add tests
def fingerprint_dataframe(df: DataFrame, chunk_size: int = 100, buckets: int = 8) -> DataFrame:
    return (
        df
        .select(fingerprint_col(df.columns, chunk_size).alias("_row_hash"))
        .withColumn("_bucket", F.pmod(F.col("_row_hash"), F.lit(buckets)))
        .groupBy("_bucket", "_row_hash")
        .agg(F.count(F.lit(1)).alias("_row_count"))
    )

def fingerprint_df(
        df: DataFrame,
        hash_fn: Callable[..., Column] = F.xxhash64,
        chunk_size: int = 100,
        buckets: int = 8
) -> DataFrame:
    logger.info(f"{len(df.schema)=}, {chunk_size=}, {buckets=}")
    return (
        df
        .select(fingerprint_col(df.columns, hash_fn, chunk_size).alias("_row_fingerprint"))
        .withColumn("_bucket", F.pmod(F.col("_row_fingerprint"), F.lit(buckets)))
        .groupBy("_bucket", "_row_fingerprint")
        .agg(F.count(F.lit(1)).alias("_row_count"))
    )


def with_row_hash_col(df: DataFrame, col_name: str = "row_hash") -> DataFrame:
    def row_hash(schema: StructType) -> Column:
        return sha2(concat_ws("||", *sorted([fieldName.upper() for fieldName in schema.fieldNames()])), 256)

    return df.withColumn(col_name, row_hash(df.schema))


def hashdiff(df1: DataFrame, df2: DataFrame) -> DataFrame:
    # Create a consistent string representation of each row and hash it
    df1_hashed = with_row_hash_col(df1).select("row_hash")
    df2_hashed = with_row_hash_col(df2).select("row_hash")

    # Count occurrences of each hash in both DataFrames
    df1_hash_counts = df1_hashed.groupBy("row_hash").count().withColumnRenamed("count", "count1")
    df2_hash_counts = df2_hashed.groupBy("row_hash").count().withColumnRenamed("count", "count2")

    # Join the hash counts
    comparison = df1_hash_counts.join(df2_hash_counts, "row_hash", "full_outer")

    # Check if any hash has different counts between the DataFrames
    diff = comparison.filter("count1 IS NULL OR count2 IS NULL OR count1 != count2")

    return diff


