from python_databricks_env.utils.fs.is_dbfs import is_dbfs


def test_is_dbfs(spark):
    assert not is_dbfs(None)
    assert not is_dbfs(spark)
