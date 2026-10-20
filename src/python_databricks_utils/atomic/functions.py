import logging
from collections.abc import Callable

from python_spark_utils.utils.utils import throw
from .Atomic import Atomic
from .AtomicResult import AtomicFailure

logger = logging.getLogger(__name__)


def map[T, R](func: Callable[[T], R], atom: Atomic[T]) -> Atomic[R]:
    return Atomic(atom.source, atom.transformations + [func])


def flatmap[T, R](func: Callable[[T], Atomic[R]], atom: Atomic[T]) -> Atomic[R]:
    try:
        t = atom()

        if isinstance(t, AtomicFailure):
            return Atomic(atom.source, [lambda *_, **__: throw()])

        atom_r = func(t())

        source = atom.source.merge(atom_r.source)

        r = atom_r()
        if isinstance(r, AtomicFailure):
            return Atomic(source, [lambda *_, **__: throw()])
        else:
            return Atomic(source, [lambda *_, **__: r()])
    except Exception as ex:
        logger.debug(f"flatmap failed: error={ex}.")
        return Atomic(atom.source, [lambda *_, **__: throw(ex)])


def flatten[T](atom: Atomic[Atomic[T]]) -> Atomic[T]:
    return flatmap(lambda a: a, atom)


def map2[T1, T2, R](func: Callable[[T1, T2], R], atom1: Atomic[T1], atom2: Atomic[T2]) -> Atomic[R]:
    return flatmap(
        lambda t1: map(
            lambda t2: func(t1, t2),
            atom2
        ),
        atom1
    )


def tuple2[T1, T2](atom1: Atomic[T1], atom2: Atomic[T2]) -> Atomic[tuple[T1, T2]]:
    return map2(lambda t1, t2: (t1, t2), atom1, atom2)


def map3[T1, T2, T3, R](
        func: Callable[[T1, T2, T3], R],
        atom1: Atomic[T1],
        atom2: Atomic[T2],
        atom3: Atomic[T3]) -> Atomic[R]:
    return flatmap(
        lambda t1: flatmap(
            lambda t2: map(
                lambda t3: func(t1, t2, t3),
                atom3
            ),
            atom2
        ),
        atom1
    )


def tuple3[T1, T2, T3](atom1: Atomic[T1], atom2: Atomic[T2], atom3: Atomic[T3]) -> Atomic[tuple[T1, T2, T3]]:
    return map3(lambda t1, t2, t3: (t1, t2, t3), atom1, atom2, atom3)
