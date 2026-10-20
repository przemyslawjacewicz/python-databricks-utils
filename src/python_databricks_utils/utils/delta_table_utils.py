import logging
from datetime import datetime

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from python_databricks_env import dbutils

logger = logging.getLogger(__name__)


# todo: add tests
def get_version(spark: SparkSession, path: str, at: datetime | None = None) -> int | None:
    history = DeltaTable.forPath(spark, path).history()
    filtered = history.filter(F.col("timestamp") <= at) if at is not None else history
    if not filtered.isEmpty():
        version = filtered.select("version").orderBy(F.col("timestamp").desc()).collect()[0][0]
    else:
        version = None
    logger.info(f"{path=}, {at=}, {version=}")
    return version

def get_df(path: str, at: datetime | None = None) -> DataFrame:
    reader = spark.read.format("delta")
    if at is not None:
        reader = reader.option("versionAsOf", get_version_at(path, at))
    return reader.load(path)

def get_last_timestamp(path: str):  # todo: add type
    if DeltaTable.isDeltaTable(spark, path):
        return (
            DeltaTable.forPath(spark, path)
            .history()
            .filter(F.col("operation") == "MERGE")
            .limit(1)
            .select("timestamp")
            .orderBy("timestamp", ascending=False)  # todo: not really necessary
            .first()[0]
        )
    else:
        return None

# todo: consider returning something 
# todo: add tests
def restore_to_version(spark: SparkSession, path: str, version: int | None) -> None:
    if version is None:
        dbutils.fs.rm(path, True)
    else:
        DeltaTable.forPath(spark, path).restoreToVersion(version)


# todo: consider returning something
# todo: add tests
def restore_at(spark: SparkSession, path: str, at: datetime | None) -> None:
    if at is None:
        dbutils.fs.rm(path, True)
    else:
        restore_to_version(spark, path, get_version(spark, path, at))

def clone_table_at(
        src_path: str,
        trg_path: str,
        at: datetime | None = None,
        is_shallow: bool = True,
        replace: bool = True
) -> DeltaTable:
    if replace and DeltaTable.isDeltaTable(spark, trg_path):
        rmdir(trg_path)

    if not DeltaTable.isDeltaTable(spark, trg_path):
        version = get_version_at(src_path, at)
        DeltaTable.forPath(spark, src_path).cloneAtVersion(version, trg_path, isShallow=is_shallow, replace=True)

    return DeltaTable.forPath(spark, trg_path)

def rw_table_at(
        src_path: str,
        trg_path: str,
        at: datetime | None = None,
        limit: int | None = None,
        replace: bool = True
) -> DeltaTable:
    if replace and DeltaTable.isDeltaTable(spark, trg_path):
        rm(trg_path)

    if not DeltaTable.isDeltaTable(spark, trg_path):
        version = get_version_at(src_path, at)
        src = spark.read.format("delta").option("versionAsOf", version).load(src_path)
        trg = src.limit(limit) if limit is not None else src
        trg.write.format("delta").save(trg_path)

    return DeltaTable.forPath(spark, trg_path)

def rw_table_fraction_by_partition_at(
        src_path: str,
        trg_path: str,
        partition_col_name: str,
        ratio: float,
        at: datetime | None = None,
        replace: bool = True
) -> DeltaTable:
    if replace and DeltaTable.isDeltaTable(spark, trg_path):
        rm(trg_path)

    if not DeltaTable.isDeltaTable(spark, trg_path):
        version = get_version_at(src_path, at)
        src = spark.read.format("delta").option("versionAsOf", version).load(src_path)

        count_by_partition_df = src.groupBy(partition_col_name).count()
        count_by_partition = {row[0]: row[1] for row in count_by_partition_df.collect()}
        logger.info(f"partition count: {len(count_by_partition)}")

        for partition in count_by_partition.keys():
            logger.info(f"{partition=}")
            df = src.filter(F.col(partition_col_name) == partition).limit(int(count_by_partition[partition] * ratio))
            df.write.format("delta").mode("append").save(trg_path)

    return DeltaTable.forPath(spark, trg_path)



# todo: add tests
def get_diff_by_col(spark: SparkSession, path: str, base_version: int, version: int | None, col: str) -> DataFrame:
    df_base = spark.read.format("delta").option("versionAsOf", base_version).load(path)
    col_value_base = df_base.select(F.max(col)).collect()[0][0]
    condition = F.col(col) > col_value_base

    if version is None:
        df = spark.read.format("delta").load(path)
    else:
        df = spark.read.format("delta").option("versionAsOf", version).load(path)

    return df.where(condition)

def get_diff_by_inc_col(path: str, from_: int | datetime, to_: int | datetime | None, col: str) -> DataFrame:
    logger.info(f"{path=}, {from_=}, {to_=}, {col=}.")

    version_from = get_version_at(path, from_) if isinstance(from_, datetime) else from_
    df_from = spark.read.format("delta").option("versionAsOf", version_from).load(path)

    col_from = df_from.select(F.max(col)).collect()[0][0]

    reader = spark.read.format("delta")
    if to_ is not None:
        version_to = get_version_at(path, to_) if isinstance(to_, datetime) else to_
        reader = reader.option("versionAsOf", version_to)
    df = reader.load(path).where(F.col(col) > col_from)

    logger.info(f"{col_from=}.")

    return df



# todo: add tests
def get_diff_by_ts(spark: SparkSession, path: str, base_version: int, version: int | None, col: str) -> DataFrame:
    col_value_base = (
        DeltaTable.forPath(spark, path).history()
        .where(F.col("version") == base_version)
        .select("timestamp")
        .collect()[0][0]
    )
    condition = F.col(col) > col_value_base

    if version is None:
        df = spark.read.format("delta").load(path)
    else:
        df = spark.read.format("delta").option("versionAsOf", version).load(path)

    return df.where(condition)

def get_diff_by_ts_col(path: str, from_: int | datetime, to_: int | datetime | None, col: str) -> DataFrame:
    logger.info(f"{path=}, {from_=}, {to_=}, {col=}.")

    version_from = get_version_at(path, from_) if isinstance(from_, datetime) else from_

    col_from = (
        DeltaTable
        .forPath(spark, path)
        .history()
        .where(F.col("version") == version_from)
        .select("timestamp")
        .collect()[0][0]
    )
    reader = spark.read.format("delta")
    if to_ is not None:
        version_to = get_version_at(path, to_) if isinstance(to_, datetime) else to_
        reader = reader.option("versionAsOf", version_to)
    df = reader.load(path).where(F.col(col) > col_from)

    logger.info(f"{col_from=}.")

    return df


# todo: add tests
def get_diff_by_cdf(spark: SparkSession, path: str, starting_version: int, ending_version: int | None) -> DataFrame:
    reader = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
    )

    if ending_version is None:
        changed = reader.load(path)
    else:
        changed = reader.option("endingVersion", ending_version).load(path)

    return (
        changed
        .where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )

def get_diff_by_cdf(path: str, starting_version: int, ending_version: int | None) -> DataFrame:
    logger.info(f"{path=}, {starting_version=}, {ending_version=}.")

    reader = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
    )

    if ending_version is not None:
        reader = reader.option("endingVersion", ending_version)
    changed = reader.load(path)

    df = (
        changed
        .where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )

    logger.info(f"{len(df.schema)=}.")

    return df

def enable_change_data_feed(path: str) -> None:
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    properties = detail["properties"]
    enabled = properties.get("delta.enableChangeDataFeed", "false").lower() == "true"
    if not enabled:
        spark.sql(f"""ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)""")


