from dotenv import load_dotenv

load_dotenv()

import os
import sys

from delta import configure_spark_with_delta_pip

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # PYSPARK_PYTHON
    os.environ["PYSPARK_PYTHON"] = sys.executable
    print(f"{os.environ['PYSPARK_PYTHON']=}")

    # PYSPARK_DRIVER_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    print(f"{os.environ['PYSPARK_DRIVER_PYTHON']=}")

    # JAVA_HOME
    if "JAVA_HOME" in os.environ:
        print(f"Using JAVA_HOME from environment: {os.environ['JAVA_HOME']}")
    else:
        print("WARNING: JAVA_HOME not found in .env or system environment.")
    print(f"{os.environ['JAVA_HOME']=}")

    builder = (
        SparkSession
        .builder
        .appName("python-databricks-env")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.rdd.compress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    spark.stop()
