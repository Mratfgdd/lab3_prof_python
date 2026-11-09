from collections.abc import Iterable, Iterator
from itertools import groupby, islice
from operator import attrgetter
from typing import Generic, TypeVar

from stream_processor.models import StudentRecord

T = TypeVar("T")


class Batcher(Generic[T]):
    def __init__(self, items: Iterable[T], size: int) -> None:
        if size < 1:
            raise ValueError("Розмір пакета має бути >= 1")
        self._iterator = iter(items)
        self._size = size

    def __iter__(self) -> "Batcher[T]":
        return self

    def __next__(self) -> list[T]:
        batch = list(islice(self._iterator, self._size))
        if not batch:
            raise StopIteration
        return batch


def batched_by_group(
    records: Iterable[StudentRecord],
    max_size: int = 100,
) -> Iterator[list[StudentRecord]]:
    for _, members in groupby(records, key=attrgetter("group")):
        yield from Batcher(members, max_size)


def batch_statistics(batch: list[StudentRecord]) -> dict[str, tuple[int, float]]:
    key = attrgetter("group")
    statistics: dict[str, tuple[int, float]] = {}
    for group, members in groupby(sorted(batch, key=key), key=key):
        grades = [member.average_grade for member in members]
        statistics[group] = (len(grades), round(sum(grades) / len(grades), 2))
    return statistics
