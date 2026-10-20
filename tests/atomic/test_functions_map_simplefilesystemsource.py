from pathlib import Path

from python_databricks_env import dbutils
from python_databricks_env.utils.fs.exists import exists

from python_spark_utils.atomic import functions
from python_spark_utils.atomic.Atomic import Atomic
from python_spark_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_spark_utils.atomic.SimpleFilesystemSouce import SimpleFilesystemSource
from python_spark_utils.utils.utils import throw


def test_functions_map_success_map1(tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(path, "0")

    get_result = functions.map(
        lambda _: put(path, "1"),
        atomic(path)
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is True
    assert collect(path) == "1"
    assert not exists(str(backup_path_creator(str(path))))


def test_functions_map_failure_map1(tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(path, "0")

    get_result = functions.map(
        lambda os: throw(),
        atomic(path)
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(path) == "0"
    assert not exists(str(backup_path_creator(str(path))))


def test_functions_map_success_map2_single_source(tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(path, "0")

    get_result = functions.map(
        lambda _: put(path, "2"),
        functions.map(
            lambda _: put(path, "1"), atomic(path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is True
    assert collect(path) == "2"
    assert not exists(str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_first_fails(tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(path, "0")

    get_result = functions.map(
        lambda _: put(path, "2"),
        functions.map(
            lambda _: throw(), atomic(path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(path) == "0"
    assert not exists(str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_second_fails(tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(path, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: put(path, "1"), atomic(path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(path) == "0"
    assert not exists(str(backup_path_creator(str(path))))


def test_functions_map_failure_map2_single_source_both_fail(tmp_path):
    path = tmp_path / "file.txt"

    # init
    put(path, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(path)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path)
    assert collect(path) == "0"
    assert not exists(str(backup_path_creator(str(path))))


def test_functions_map_success_map2_two_sources(tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(path1, "0")
    put(path2, "0")

    get_result = functions.map(
        lambda _: put(path2, "1"),
        functions.map(
            lambda _: put(path1, "1"), atomic(path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() is True

    assert collect(path1) == "1"
    assert not exists(str(backup_path_creator(str(path1))))

    assert collect(path2) == "1"
    assert not exists(str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_first_fails(tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(path1, "0")
    put(path2, "0")

    get_result = functions.map(
        lambda _: put(path2, "1"),
        functions.map(
            lambda _: throw(), atomic(path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert collect(path1) == "0"
    assert not exists(str(backup_path_creator(str(path1))))

    assert collect(path2) == "0"
    assert not exists(str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_second_fails(tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(path1, "0")
    put(path2, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: put(path1, "1"), atomic(path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert collect(path1) == "0"
    assert not exists(str(backup_path_creator(str(path1))))

    assert collect(path2) == "0"
    assert not exists(str(backup_path_creator(str(path2))))


def test_functions_map_failure_map2_two_sources_both_fails(tmp_path):
    path1 = tmp_path / "file1.txt"
    path2 = tmp_path / "file2.txt"

    # init
    put(path1, "0")
    put(path2, "0")

    get_result = functions.map(
        lambda _: throw(),
        functions.map(
            lambda _: throw(), atomic(path1)
        )
    )
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == str(path1)

    assert collect(path1) == "0"
    assert not exists(str(backup_path_creator(str(path1))))

    assert collect(path2) == "0"
    assert not exists(str(backup_path_creator(str(path2))))


def atomic(path: Path) -> Atomic[Path]:
    return Atomic(SimpleFilesystemSource(str(path), backup_path_creator))


def backup_path_creator(s: str) -> str:
    path = Path(s)
    parent = path.parent
    name = path.name
    return str(parent / f"{name}.bck")


def put(path: Path, contents: str) -> bool:
    return dbutils.fs.put(str(path), contents, True)


def collect(path: Path) -> str:
    return dbutils.fs.head(str(path))
