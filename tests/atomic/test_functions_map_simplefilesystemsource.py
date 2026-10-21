from pathlib import Path

from python_databricks_utils.atomic import functions
from python_databricks_utils.atomic.Atomic import Atomic
from python_databricks_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_databricks_utils.atomic.SimpleFilesystemSouce import SimpleFilesystemSource
from python_databricks_utils.databricks.dbutils.DbUtilsLike import DbUtilsLike
from python_databricks_utils.utils.dbfs_utils import exists
from python_databricks_utils.utils.utils import throw


def test_functions_map_success_map1(dbutils, tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(dbutils, path, "0")

    get_result = functions.map(
        lambda _: put(dbutils, path, "1"),
        atomic(dbutils, path)
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is True
    assert collect(dbutils, path) == "1"
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map1(dbutils, tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(dbutils, path, "0")

    get_result = functions.map(
        lambda os: throw(),
        atomic(dbutils, path)
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(dbutils, path) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_success_map2_single_source(dbutils, tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(dbutils, path, "0")

    get_result = functions.map(
        lambda _: put(dbutils, path, "2"),
        functions.map(
            lambda _: put(dbutils, path, "1"), atomic(dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is True
    assert collect(dbutils, path) == "2"
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_first_fails(dbutils, tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(dbutils, path, "0")

    get_result = functions.map(
        lambda _: put(dbutils, path, "2"),
        functions.map(
            lambda _: throw(), atomic(dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(dbutils, path) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_second_fails(dbutils, tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(dbutils, path, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: put(dbutils, path, "1"), atomic(dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(dbutils, path) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_both_fail(dbutils, tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(dbutils, path, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(dbutils, path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(dbutils, path) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path))))


def test_functions_map_success_map2_two_sources(dbutils, tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(dbutils, path1, "0")
    put(dbutils, path2, "0")

    get_result = functions.map(
        lambda _: put(dbutils, path2, "1"),
        functions.map(
            lambda _: put(dbutils, path1, "1"), atomic(dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is True

    assert collect(dbutils, path1) == "1"
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert collect(dbutils, path2) == "1"
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_first_fails(dbutils, tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(dbutils, path1, "0")
    put(dbutils, path2, "0")

    get_result = functions.map(
        lambda _: put(dbutils, path2, "1"),
        functions.map(
            lambda _: throw(), atomic(dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert collect(dbutils, path1) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert collect(dbutils, path2) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_second_fails(dbutils, tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(dbutils, path1, "0")
    put(dbutils, path2, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: put(dbutils, path1, "1"), atomic(dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert collect(dbutils, path1) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert collect(dbutils, path2) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_both_fails(dbutils, tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(dbutils, path1, "0")
    put(dbutils, path2, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(dbutils, path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert collect(dbutils, path1) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path1))))

    assert collect(dbutils, path2) == "0"
    assert not exists(dbutils, str(backup_path_creator(str(path2))))


def atomic(dbutils: DbUtilsLike, path: Path) -> Atomic[Path]:
    return Atomic(SimpleFilesystemSource(dbutils, str(path), backup_path_creator))


def backup_path_creator(s: str) -> str:
    path = Path(s)
    parent = path.parent
    name = path.name
    return str(parent / f"{name}.bck")


def put(dbutils: DbUtilsLike, path: Path, contents: str) -> bool:
    return dbutils.fs.put(str(path), contents, True)


def collect(dbutils: DbUtilsLike, path: Path) -> str:
    return dbutils.fs.head(str(path))
