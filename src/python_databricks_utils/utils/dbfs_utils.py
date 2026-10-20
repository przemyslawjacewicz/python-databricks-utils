import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession

from python_databricks_utils.databricks.dbutils.DbUtilsLike import DbUtilsLike
from python_databricks_utils.logging.logging import get_logger

logger = get_logger()


def exists(path: str, dbutils: DbUtilsLike) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as ex:
        logger.error(f"Error while checking if path exists: {ex}")
        return False


def is_dbfs(spark: SparkSession) -> bool:
    try:
        return spark.sparkContext.getConf().get("spark.databricks.clusterUsageTags.clusterId") is not None
    except Exception as ex:
        logger.error(f"Error checking if on DBFS env: {ex}")
        return False


def resolve(spark: SparkSession, *parts: str, is_dbfs_fn: Callable[[SparkSession], bool] = is_dbfs) -> str:
    joined = "/".join(parts)
    normalized = re.sub(r"/+", "/", joined).rstrip("/")

    if is_dbfs_fn(spark):
        return f"dbfs:{normalized}" if not normalized.startswith("dbfs:") else normalized
    else:
        return normalized


def rm_hdfs(spark: SparkSession, dir: str) -> bool:
    logger.info(f"{dir=}")

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    path = jvm.org.apache.hadoop.fs.Path(dir)

    deleted = fs.delete(path, True)

    logger.info(f"{deleted=}")

    return deleted


def rm(dbutils: DbUtilsLike, dir: str, max_workers: int = 32) -> None:
    logger.info(f"{dir=}, {max_workers=}")

    def rm_path(path: str) -> tuple[str, Exception] | None:
        try:
            deleted = dbutils.fs.rm(path, True)
            if not deleted:
                return path, Exception(f"Failed to remove {path}")
            return None
        except Exception as e:
            return path, e

    paths = [f.path for f in dbutils.fs.ls(dir)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        fs = [executor.submit(rm_path, path) for path in paths]

        errors = [r.result() for r in as_completed(fs) if r.result() is not None]

    if errors:
        raise Exception(f"Failed to remove {dir}: {errors}")

    deleted = dbutils.fs.rm(dir, True)
    if not deleted:
        raise Exception(f"Failed to remove {dir}")
