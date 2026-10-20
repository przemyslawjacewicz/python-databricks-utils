from python_spark_utils.atomic.Atomic import Atomic
from python_spark_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_spark_utils.atomic.SimpleSource import SimpleSource
from python_spark_utils.atomic.functions import map2
from python_spark_utils.utils.utils import throw


def test_functions_map2_success():
    get_result = map2(
        lambda t1, t2: list(zip(t1, t2)),
        atomic([0]),
        atomic(["a"])
    )
    result = get_result()
    assert isinstance(result, AtomicSuccess)
    assert result() == [(0, "a")]


def test_functions_map2_failure():
    get_result = map2(
        lambda t1, t2: throw(),
        atomic([0]),
        atomic(["a"])
    )
    result = get_result()
    assert isinstance(result, AtomicFailure)
    assert result() == [[0], ["a"]]


def atomic[S](source: S) -> Atomic[S]:
    return Atomic(SimpleSource(source))
