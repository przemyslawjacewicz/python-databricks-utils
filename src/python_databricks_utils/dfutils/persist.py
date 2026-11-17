import logging
import random
import string
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from python_databricks_utils.utils.dbfs_utils import resolve

logger = logging.getLogger()


def persist(
        spark: SparkSession,
        df: DataFrame,
        root_path: str,
        dirname_gen: Callable[[], str] = lambda: "".join(random.choices(string.ascii_letters + string.digits, k=16)),
        write: Callable[[DataFrame, str], None] = lambda _df, _p: _df.write.format("parquet").save(_p),
        read: Callable[[SparkSession, str], DataFrame] = lambda _s, _p: _s.read.format("parquet").load(_p)
) -> DataFrame:
    logging.debug(f"{root_path=}")

    dirname = dirname_gen()
    path = resolve(spark, root_path, dirname)
    write(df, path)
    r = read(spark, path)

    logger.debug(f"{path=}")
    return r
