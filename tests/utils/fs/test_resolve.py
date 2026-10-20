from python_databricks_env.utils.fs.resolve import resolve


def test_resolve_local(spark):
    assert resolve("/path/to/file", dbfs=lambda _: False) == "/path/to/file"
    assert resolve("/path/to/file/", dbfs=lambda _: False) == "/path/to/file"

    assert resolve("/path/", "/to/", "/file/", dbfs=lambda _: False) == "/path/to/file"
    assert resolve("/path", "to", "file", dbfs=lambda _: False) == "/path/to/file"

    assert resolve("path/to/file", dbfs=lambda _: False) == "path/to/file"
    assert resolve("path/to/file/", dbfs=lambda _: False) == "path/to/file"

    assert resolve("path/", "/to/", "/file/", dbfs=lambda _: False) == "path/to/file"
    assert resolve("path", "to", "file", dbfs=lambda _: False) == "path/to/file"


def test_resolve_dbfs(spark):
    assert resolve("/path/to/file", dbfs=lambda _: True) == "dbfs:/path/to/file"
    assert resolve("/path/to/file/", dbfs=lambda _: True) == "dbfs:/path/to/file"

    assert resolve("/path/", "/to/", "/file/", dbfs=lambda _: True) == "dbfs:/path/to/file"
    assert resolve("/path", "to", "file", dbfs=lambda _: True) == "dbfs:/path/to/file"

    assert resolve("path/to/file", dbfs=lambda _: True) == "dbfs:path/to/file"
    assert resolve("path/to/file/", dbfs=lambda _: True) == "dbfs:path/to/file"

    assert resolve("path/", "/to/", "/file/", dbfs=lambda _: True) == "dbfs:path/to/file"
    assert resolve("path", "to", "file", dbfs=lambda _: True) == "dbfs:path/to/file"
