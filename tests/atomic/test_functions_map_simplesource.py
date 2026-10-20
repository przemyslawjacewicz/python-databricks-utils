from python_spark_utils.atomic import functions
from python_spark_utils.atomic.Atomic import Atomic
from python_spark_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_spark_utils.atomic.SimpleSource import SimpleSource
from python_spark_utils.utils.utils import throw


def test_functions_map_success_map1():
    get_result = functions.map(
        lambda os: os + [1],
        atomic([0])
    )
    result = get_result()
    assert isinstance(result, AtomicSuccess)
    assert result() == [0, 1]


def test_functions_map_failure_map1():
    get_result = functions.map(
        lambda os: throw(),
        atomic([0])
    )
    result = get_result()
    assert isinstance(result, AtomicFailure)
    assert result() == [0]


def test_functions_map_success_map2_single_source():
    get_result = functions.map(
        lambda os: os + [2],
        functions.map(
            lambda os: os + [1],
            atomic([0])
        )
    )
    result = get_result()
    assert isinstance(result, AtomicSuccess)
    assert result() == [0, 1, 2]


def test_functions_map_failure_map2_single_source_first_fails():
    get_result = functions.map(
        lambda os: os + [2],
        functions.map(
            lambda os: throw(),
            atomic([0])
        )
    )
    result = get_result()
    assert isinstance(result, AtomicFailure)
    assert result() == [0]


def test_functions_map_failure_map2_single_source_second_fails():
    get_result = functions.map(
        lambda os: throw(),
        functions.map(
            lambda os: os + [1],
            atomic([0])
        )
    )
    result = get_result()
    assert isinstance(result, AtomicFailure)
    assert result() == [0]


def test_functions_map_failure_map2_single_source_both_fail():
    get_result = functions.map(
        lambda os: throw(),
        functions.map(
            lambda os: throw(),
            atomic([0])
        )
    )
    result = get_result()
    assert isinstance(result, AtomicFailure)
    assert result() == [0]


def atomic[S](source: S) -> Atomic[S]:
    return Atomic(SimpleSource(source))
