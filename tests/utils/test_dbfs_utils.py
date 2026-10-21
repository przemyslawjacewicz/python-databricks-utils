import os

from python_databricks_utils.utils.dbfs_utils import exists, is_dbfs, resolve


def test_exists(dbutils):
    assert exists(dbutils, str(os.path.dirname(os.environ["VIRTUAL_ENV"])))
    assert not exists(dbutils, "/not/exists")


def test_is_dbfs(spark):
    assert not is_dbfs(spark)


def test_resolve(spark):
    # local
    assert resolve(spark, "/", is_dbfs_fn=lambda _: False) == "/"

    assert resolve(spark, "/path/to/file", is_dbfs_fn=lambda _: False) == "/path/to/file"
    assert resolve(spark, "/path/to/file/", is_dbfs_fn=lambda _: False) == "/path/to/file"

    assert resolve(spark, "/path/", "/to/", "/file/", is_dbfs_fn=lambda _: False) == "/path/to/file"
    assert resolve(spark, "/path", "to", "file", is_dbfs_fn=lambda _: False) == "/path/to/file"

    assert resolve(spark, "path/to/file", is_dbfs_fn=lambda _: False) == "path/to/file"
    assert resolve(spark, "path/to/file/", is_dbfs_fn=lambda _: False) == "path/to/file"

    assert resolve(spark, "path/", "/to/", "/file/", is_dbfs_fn=lambda _: False) == "path/to/file"
    assert resolve(spark, "path", "to", "file", is_dbfs_fn=lambda _: False) == "path/to/file"

    # dbfs
    assert resolve(spark, "/", is_dbfs_fn=lambda _: True) == "dbfs:/"

    assert resolve(spark, "/path/to/file", is_dbfs_fn=lambda _: True) == "dbfs:/path/to/file"
    assert resolve(spark, "/path/to/file/", is_dbfs_fn=lambda _: True) == "dbfs:/path/to/file"

    assert resolve(spark, "/path/", "/to/", "/file/", is_dbfs_fn=lambda _: True) == "dbfs:/path/to/file"
    assert resolve(spark, "/path", "to", "file", is_dbfs_fn=lambda _: True) == "dbfs:/path/to/file"

    assert resolve(spark, "path/to/file", is_dbfs_fn=lambda _: True) == "dbfs:path/to/file"
    assert resolve(spark, "path/to/file/", is_dbfs_fn=lambda _: True) == "dbfs:path/to/file"

    assert resolve(spark, "path/", "/to/", "/file/", is_dbfs_fn=lambda _: True) == "dbfs:path/to/file"
    assert resolve(spark, "path", "to", "file", is_dbfs_fn=lambda _: True) == "dbfs:path/to/file"
