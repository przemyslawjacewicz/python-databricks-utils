import logging
from datetime import datetime

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from python_databricks_utils.databricks.dbutils.DbUtilsLike import DbUtilsLike
from python_databricks_utils.utils.dbfs_utils import rm, exists

logger = logging.getLogger(__name__)


def clone_table(
        spark: SparkSession,
        dbutils: DbUtilsLike,
        source_path: str,
        target_path: str,
        at: datetime | None = None,
        is_shallow: bool = True,
        replace: bool = True
) -> DeltaTable:
    if replace and DeltaTable.isDeltaTable(spark, target_path):
        rm(dbutils, target_path)

    if not DeltaTable.isDeltaTable(spark, target_path):
        version = get_version(spark, source_path, at)
        DeltaTable.forPath(spark, source_path).cloneAtVersion(version, target_path, isShallow=is_shallow, replace=True)

    return DeltaTable.forPath(spark, target_path)


def get_df(spark: SparkSession, path: str, at: datetime | None = None) -> DataFrame | None:
    reader = spark.read.format("delta")

    if at is not None:
        version = get_version(spark, path, at)
        if version is not None:
            return reader.option("versionAsOf", version).load(path)
        else:
            return None
    else:
        return reader.load(path)


def get_df_or_empty(
        spark: SparkSession,
        dbutils: DbUtilsLike,
        path: str,
        schema: StructType,
        at: datetime | None = None
) -> DataFrame | None:
    if exists(dbutils, path):
        return get_df(spark, path, at)
    else:
        return spark.createDataFrame([], schema)


def get_last_timestamp(spark: SparkSession, path: str, operation: str) -> datetime:
    return (
        DeltaTable.forPath(spark, path)
        .history()
        .filter(F.col("operation") == operation)
        .orderBy("timestamp", ascending=False)  # todo: not really necessary
        .limit(1)
        .select("timestamp")
        .collect()[0][0]
    )


def get_version(spark: SparkSession, path: str, at: datetime | None = None) -> int | None:
    history = DeltaTable.forPath(spark, path).history()

    if at is not None:
        filtered = history.filter(F.col("timestamp") <= at)
    else:
        filtered = history

    if not filtered.isEmpty():
        version = filtered.select("version").orderBy(F.col("timestamp").desc()).collect()[0][0]
    else:
        version = None

    logger.info(f"{path=}, {at=}, {version=}")
    return version


# todo: consider returning something
def restore_at(spark: SparkSession, dbutils: DbUtilsLike, path: str, at: datetime | None) -> None:
    if at is not None:
        restore_to_version(spark, dbutils, path, get_version(spark, path, at))
    else:
        dbutils.fs.rm(path, True)


# todo: consider returning something
def restore_to_version(spark: SparkSession, dbutils: DbUtilsLike, path: str, version: int | None) -> None:
    if version is not None:
        DeltaTable.forPath(spark, path).restoreToVersion(version)
    else:
        dbutils.fs.rm(path, True)


def rw_table(
        spark: SparkSession,
        dbutils: DbUtilsLike,
        src_path: str,
        trg_path: str,
        at: datetime | None = None,
        limit: int | None = None,
        replace: bool = True
) -> DeltaTable:
    if replace and DeltaTable.isDeltaTable(spark, trg_path):
        rm(dbutils, trg_path)

    if not DeltaTable.isDeltaTable(spark, trg_path):
        version = get_version(spark, src_path, at)
        src = spark.read.format("delta").option("versionAsOf", version).load(src_path)
        trg = src.limit(limit) if limit is not None else src
        trg.write.format("delta").save(trg_path)

    return DeltaTable.forPath(spark, trg_path)


def rw_table_fraction_by_partition_at(
        spark: SparkSession,
        dbutils: DbUtilsLike,
        src_path: str,
        trg_path: str,
        partition_col_name: str,
        ratio: float,
        at: datetime | None = None,
        replace: bool = True
) -> DeltaTable:
    if replace and DeltaTable.isDeltaTable(spark, trg_path):
        rm(dbutils, trg_path)

    if not DeltaTable.isDeltaTable(spark, trg_path):
        version = get_version(spark, src_path, at)
        src = spark.read.format("delta").option("versionAsOf", version).load(src_path)

        count_by_partition_df = src.groupBy(partition_col_name).count()
        count_by_partition = {row[0]: row[1] for row in count_by_partition_df.collect()}
        logger.info(f"partition count: {len(count_by_partition)}")

        for partition in count_by_partition.keys():
            logger.info(f"{partition=}")
            df = src.filter(F.col(partition_col_name) == partition).limit(int(count_by_partition[partition] * ratio))
            df.write.format("delta").mode("append").save(trg_path)

    return DeltaTable.forPath(spark, trg_path)


def get_diff_by_inc_col(
        spark: SparkSession,
        path: str,
        from_: int | datetime,
        to_: int | datetime | None,
        col: str
) -> DataFrame:
    logger.info(f"{path=}, {from_=}, {to_=}, {col=}")

    version_from = get_version(spark, path, from_) if isinstance(from_, datetime) else from_
    df_from = spark.read.format("delta").option("versionAsOf", version_from).load(path)

    col_from = df_from.select(F.max(col)).collect()[0][0]

    reader = spark.read.format("delta")
    if to_ is not None:
        version_to = get_version(spark, path, to_) if isinstance(to_, datetime) else to_
        reader = reader.option("versionAsOf", version_to)
    df = reader.load(path).where(F.col(col) > col_from)

    logger.info(f"{col_from=}")

    return df


def get_diff_by_version_timestamp(
        spark: SparkSession,
        path: str,
        from_: int | datetime,
        to_: int | datetime | None,
        col: str
) -> DataFrame:
    logger.info(f"{path=}, {from_=}, {to_=}, {col=}")

    version_from = get_version(spark, path, from_) if isinstance(from_, datetime) else from_

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
        version_to = get_version(spark, path, to_) if isinstance(to_, datetime) else to_
        reader = reader.option("versionAsOf", version_to)
    df = reader.load(path).where(F.col(col) > col_from)

    logger.info(f"{col_from=}")

    return df


def get_diff_by_cdf(spark: SparkSession, path: str, starting_version: int, ending_version: int | None) -> DataFrame:
    logger.info(f"{path=}, {starting_version=}, {ending_version=}")

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

    logger.info(f"{len(df.schema)=}")

    return df


def enable_change_data_feed(spark: SparkSession, path: str) -> None:
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    properties = detail["properties"]
    enabled = properties.get("delta.enableChangeDataFeed", "false").lower() == "true"
    if not enabled:
        spark.sql(f"""ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
