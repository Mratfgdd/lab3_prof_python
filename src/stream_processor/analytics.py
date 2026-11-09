import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from itertools import accumulate
from typing import NamedTuple

from stream_processor.models import StudentRecord


class Summary(NamedTuple):
    records: int
    average: float
    minimum: float
    maximum: float
    best_student: str | None
    by_group: Counter[str]
    group_averages: dict[str, float]


def summarize(records: Iterable[StudentRecord]) -> Summary:
    count = 0
    total = 0.0
    minimum, maximum = math.inf, -math.inf
    best: StudentRecord | None = None
    by_group: Counter[str] = Counter()
    group_totals: defaultdict[str, float] = defaultdict(float)
    for record in records:
        count += 1
        total += record.average_grade
        minimum = min(minimum, record.average_grade)
        if best is None or record.average_grade > best.average_grade:
            best = record
            maximum = record.average_grade
        by_group[record.group] += 1
        group_totals[record.group] += record.average_grade
    return Summary(
        records=count,
        average=round(total / count, 2) if count else 0.0,
        minimum=minimum if count else 0.0,
        maximum=maximum if count else 0.0,
        best_student=best.full_name if best else None,
        by_group=by_group,
        group_averages={
            group: round(group_totals[group] / members, 2) for group, members in by_group.items()
        },
    )


def streaming_average(values: Iterable[float]) -> float:
    total = 0.0
    count = 0
    for value in values:
        total += value
        count += 1
    if count == 0:
        return 0.0
    return round(total / count, 2)


def min_max_grade(records: Iterable[StudentRecord]) -> tuple[StudentRecord, StudentRecord] | None:
    lowest = highest = None
    for record in records:
        if lowest is None or highest is None:
            lowest = highest = record
            continue
        if record.average_grade < lowest.average_grade:
            lowest = record
        if record.average_grade > highest.average_grade:
            highest = record
    if lowest is None or highest is None:
        return None
    return lowest, highest


def count_by_group(records: Iterable[StudentRecord]) -> Counter[str]:
    return Counter(record.group for record in records)


def running_average(records: Iterable[StudentRecord]) -> Iterator[float]:
    grades = (record.average_grade for record in records)
    return (round(total / n, 2) for n, total in enumerate(accumulate(grades), 1))
