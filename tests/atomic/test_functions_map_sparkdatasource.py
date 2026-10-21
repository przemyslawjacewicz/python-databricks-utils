from pathlib import Path

from pyspark import Row

from python_databricks_utils.atomic import functions
from python_databricks_utils.atomic.Atomic import Atomic
from python_databricks_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_databricks_utils.atomic.SparkDataSource import SparkDataSource
from python_databricks_utils.utils.dbfs_utils import exists
from python_databricks_utils.utils.utils import throw


def test_functions_map_success_map1(spark, dbutils, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path, 1, 2),
        atomic(spark, dbutils, path)
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is None
    assert sorted(collect(spark, path)) == [(0,), (1,)]
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map1(spark, dbutils, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda os: throw(),
        atomic(spark, dbutils, path)
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_success_map2_single_source(spark, dbutils, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path, 2, 3),
        functions.map(
            lambda _: append(spark, path, 1, 2), atomic(spark, dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is None
    assert sorted(collect(spark, path)) == [(0,), (1,), (2,)]
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_first_fails(spark, dbutils, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path, 2, 3),
        functions.map(
            lambda _: throw(), atomic(spark, dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_second_fails(spark, dbutils, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: append(spark, path, 1, 2), atomic(spark, dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_both_fail(spark, dbutils, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(spark, dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_success_map2_two_sources(spark, dbutils, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path2, 1, 2),
        functions.map(
            lambda _: append(spark, path1, 1, 2), atomic(spark, dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is None

    assert sorted(collect(spark, path1)) == [(0,), (1,)]
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert sorted(collect(spark, path2)) == [(0,), (1,)]
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_first_fails(spark, dbutils, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path2, 1, 2),
        functions.map(
            lambda _: throw(), atomic(spark, dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert sorted(collect(spark, path1)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert sorted(collect(spark, path2)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_second_fails(spark, dbutils, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: append(spark, path1, 1, 2), atomic(spark, dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert sorted(collect(spark, path1)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert sorted(collect(spark, path2)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_both_fails(spark, dbutils, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(spark, dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert sorted(collect(spark, path1)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert sorted(collect(spark, path2)) == [(0,)]
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def atomic(spark, dbutils, path: Path) -> Atomic[Path]:
    return Atomic(SparkDataSource(spark, dbutils, str(path), "parquet", backup_path_creator))


def backup_path_creator(s: str) -> str:
    path = Path(s)
    parent = path.parent
    name = path.name
    return str(parent / f"{name}.bck")


def append(spark, path: Path, start: int, end: int) -> None:
    spark.range(start, end).write.format("parquet").mode("append").save(str(path))


def collect(spark, path: Path) -> list[Row]:
    return spark.read.format("parquet").load(str(path)).collect()
