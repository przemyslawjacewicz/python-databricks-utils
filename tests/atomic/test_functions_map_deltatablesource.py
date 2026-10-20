from pathlib import Path

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark import Row

from python_spark_utils.atomic import functions
from python_spark_utils.atomic.Atomic import Atomic
from python_spark_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_spark_utils.atomic.DeltaTableSource import DeltaTableSource
from python_spark_utils.utils.utils import throw


def test_functions_map_success_map1(spark, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path, 1, 2),
        atomic(spark, path)
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is None
    assert sorted(collect(spark, path)) == [(0,), (1,)]
    assert get_operations(spark, path) == ["WRITE", "WRITE"]


def test_functions_map_failure_map1(spark, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda os: throw(),
        atomic(spark, path)
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert get_operations(spark, path) == ["WRITE"]


def test_functions_map_success_map2_single_source(spark, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path, 2, 3),
        functions.map(
            lambda _: append(spark, path, 1, 2), atomic(spark, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is None
    assert sorted(collect(spark, path)) == [(0,), (1,), (2,)]
    assert get_operations(spark, path) == ["WRITE", "WRITE", "WRITE"]


def test_functions_map_failure_map2_single_source_first_fails(spark, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path, 2, 3),
        functions.map(
            lambda _: throw(), atomic(spark, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert get_operations(spark, path) == ["WRITE"]


def test_functions_map_failure_map2_single_source_second_fails(spark, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: append(spark, path, 1, 2), atomic(spark, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert get_operations(spark, path) == ["WRITE", "WRITE", "RESTORE"]


def test_functions_map_failure_map2_single_source_both_fail(spark, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(spark, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert get_operations(spark, path) == ["WRITE"]


def test_functions_map_success_map2_two_sources(spark, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path2, 1, 2),
        functions.map(
            lambda _: append(spark, path1, 1, 2), atomic(spark, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is None

    assert sorted(collect(spark, path1)) == [(0,), (1,)]
    assert get_operations(spark, path1) == ["WRITE", "WRITE"]

    assert sorted(collect(spark, path2)) == [(0,), (1,)]
    assert get_operations(spark, path2) == ["WRITE", "WRITE"]


def test_functions_map_failure_map2_two_sources_first_fails(spark, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: append(spark, path2, 1, 2),
        functions.map(
            lambda _: throw(), atomic(spark, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert sorted(collect(spark, path1)) == [(0,)]
    assert get_operations(spark, path1) == ["WRITE"]

    assert sorted(collect(spark, path2)) == [(0,)]
    assert get_operations(spark, path2) == ["WRITE"]


def test_functions_map_failure_map2_two_sources_second_fails(spark, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: append(spark, path1, 1, 2), atomic(spark, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert sorted(collect(spark, path1)) == [(0,)]
    assert get_operations(spark, path1) == ["WRITE", "WRITE", "RESTORE"]

    assert sorted(collect(spark, path2)) == [(0,)]
    assert get_operations(spark, path2) == ["WRITE"]


def test_functions_map_failure_map2_two_sources_both_fails(spark, tmp_path):
    path1 = tmp_path / "table1"
    path2 = tmp_path / "table2"

    # init
    append(spark, path1, 0, 1)
    append(spark, path2, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(spark, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert sorted(collect(spark, path1)) == [(0,)]
    assert get_operations(spark, path1) == ["WRITE"]

    assert sorted(collect(spark, path2)) == [(0,)]
    assert get_operations(spark, path2) == ["WRITE"]


def test_functions_map_failure_map3_single_source_third_fails(spark, tmp_path):
    path = tmp_path / "table"

    # init
    append(spark, path, 0, 1)

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: append(spark, path, 2, 3),
            functions.map(
                lambda _: append(spark, path, 1, 2), atomic(spark, path)
            )
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert sorted(collect(spark, path)) == [(0,)]
    assert get_operations(spark, path) == ["WRITE", "WRITE", "WRITE", "RESTORE"]


def atomic(spark, path: Path) -> Atomic[Path]:
    return Atomic(spark, DeltaTableSource(spark, str(path)))


def append(spark, path: Path, start: int, end: int) -> None:
    spark.range(start, end).write.format("delta").mode("append").save(str(path))


def collect(spark, path: Path) -> list[Row]:
    return spark.read.format("delta").load(str(path)).collect(spark)


def get_operations(spark, path: Path) -> list[str]:
    return list(
        map(
            lambda r: r["operation"],
            DeltaTable.forPath(spark, str(path)).history().orderBy(F.asc("timestamp")).collect()
        )
    )
