from pyspark.sql import SparkSession

from python_databricks_utils.utils.delta_table_utils import get_version, restore_to_version
from .BackupableSource import BackupableSource
from ..databricks.dbutils.DbUtilsLike import DbUtilsLike


class DeltaTableSource(BackupableSource[str]):
    def __init__(self, spark: SparkSession, dbutils: DbUtilsLike, source: str):
        super().__init__(source)
        self.spark = spark
        self.dbutils = dbutils

    def backup(self) -> None:
        self.backup_ref = get_version(self.spark, self.source)

    def restore(self) -> None:
        restore_to_version(self.spark, self.dbutils, self.source, self.backup_ref)

    def cleanup(self) -> None:
        return
