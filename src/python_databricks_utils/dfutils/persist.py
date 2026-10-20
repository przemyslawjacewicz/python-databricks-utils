import logging
import random
import string
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from python_databricks_env.utils.fsutils import resolve

logger = logging.getLogger(__name__)


# todo: add tests
def persist(
        spark: SparkSession,
        df: DataFrame,
        root_path: str,
        name_gen: Callable[[], str] = lambda: "".join(random.choices(string.ascii_letters + string.digits, k=16)),
        write: Callable[[DataFrame, str], None] = lambda _df, _p: _df.write.format("parquet").save(_p),
        read: Callable[[SparkSession, str], DataFrame] = lambda _s, _p: _s.read.format("parquet").load(_p)
) -> DataFrame:
    logging.debug(f"{root_path=}.")
    name = name_gen()
    path = resolve(spark, root_path, name)
    write(df, path)
    r = read(spark, path)
    logger.debug(f"{path=}.")
    return r

def persist(df: DataFrame, root_path: str) -> DataFrame:
    logger.info(f"{root_path=}.")
    name = "".join(random.choices(string.ascii_letters + string.digits, k=16))
    path = f"{root_path}/{name}"

    df.write.format("parquet").save(path)
    r = spark.read.format("parquet").load(path)

    logger.info(f"{path=}.")

    return r
