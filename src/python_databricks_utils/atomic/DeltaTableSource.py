from pyspark.sql import SparkSession

from python_spark_utils.utils.delta_table_utils import get_version, restore_to_version
from .BackupableSource import BackupableSource


class DeltaTableSource(BackupableSource[str]):
    def __init__(self, spark: SparkSession, source: str):
        super().__init__(source)
        self.spark = spark

    def backup(self) -> None:
        self.backup_ref = get_version(self.spark, self.source)

    def restore(self) -> None:
        restore_to_version(self.spark, self.source, self.backup_ref)

    def cleanup(self) -> None:
        return
