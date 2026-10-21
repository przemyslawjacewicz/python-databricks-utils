from collections.abc import Callable

from pyspark.sql import SparkSession

from .GenericFilesystemSource import GenericFilesystemSource
from ..databricks.dbutils.DbUtilsLike import DbUtilsLike


class SparkDataSource(GenericFilesystemSource):
    def __init__(self, spark: SparkSession, dbutils: DbUtilsLike, source: str, format: str,
                 backup_path: Callable[[str], str]):
        super().__init__(
            dbutils,
            source,
            backup_path,
            lambda _source, _backup_path: spark.read.format(format).load(_source).write.format(format).mode(
                "overwrite").save(_backup_path),
        )
