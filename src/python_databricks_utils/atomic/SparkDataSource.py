from collections.abc import Callable

from pyspark.sql import SparkSession

from .GenericFilesystemSource import GenericFilesystemSource


class SparkDataSource(GenericFilesystemSource):
    def __init__(self, spark: SparkSession, source: str, format: str, backup_path: Callable[[str], str]):
        super().__init__(
            source,
            lambda src, dst: spark.read.format(format).load(src).write.format(format).mode("overwrite").save(dst),
            backup_path
        )
