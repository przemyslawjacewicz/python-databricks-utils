from python_spark_utils.atomic.Atomic import Atomic
from python_spark_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_spark_utils.atomic.SimpleSource import SimpleSource
from python_spark_utils.atomic.functions import flatmap
from python_spark_utils.utils.utils import throw


def test_functions_flatmap_success_flatmap1():
    get_result = flatmap(
        lambda os: atomic(os + [1]),
        atomic([0])
    )
    result = get_result()
    assert isinstance(result, AtomicSuccess)
    assert result() == [0, 1]


def test_functions_flatmap_success_flatmap2():
    get_result = flatmap(
        lambda os: atomic(os + [2]),
        flatmap(
            lambda os: atomic(os + [1]),
            atomic([0])
        )
    )
    result = get_result()
    assert isinstance(result, AtomicSuccess)
    assert result() == [0, 1, 2]


def test_functions_flatmap_failure_flatmap1():
    get_result = flatmap(
        lambda os: throw(),
        atomic([0])
    )
    result = get_result()
    assert isinstance(result, AtomicFailure)
    assert result() == [0]


def test_functions_flatmap_failure_flatmap2():
    get_result = flatmap(
        lambda os: throw(),
        flatmap(
            lambda os: atomic([1]),
            atomic([0])
        )
    )
    result = get_result()
    assert isinstance(result, AtomicFailure)
    assert result() == [[0], [1]]


def atomic[S](source: S) -> Atomic[S]:
    return Atomic(SimpleSource(source))
