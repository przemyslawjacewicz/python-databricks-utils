import os

from python_databricks_env.utils.fs.exists import exists


def test_exists(spark):
    assert exists(str(os.path.dirname(os.environ["VIRTUAL_ENV"])))
    assert not exists("/not/exists")
