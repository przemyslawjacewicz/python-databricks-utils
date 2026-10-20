from python_spark_utils.atomic.Atomic import Atomic

from python_spark_utils.atomic.AtomicResult import AtomicSuccess, AtomicFailure
from python_spark_utils.atomic.SimpleSource import SimpleSource
from python_spark_utils.utils.utils import throw


def test_atomic_success():
    transformations = [lambda t: t + [1], lambda t: t + [2]]  # lambda _: [0],

    get_result = Atomic(SimpleSource([0]), transformations)
    result = get_result()

    assert isinstance(result, AtomicSuccess)
    assert result() == [0, 1, 2]


def test_atomic_failure():
    transformations = [lambda t: t + [1], lambda t: t + [2], lambda _: throw()]  # lambda _: [0],

    get_result = Atomic(SimpleSource([0]), transformations)
    result = get_result()

    assert isinstance(result, AtomicFailure)
    assert result() == [0]
