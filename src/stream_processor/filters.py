import math
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from heapq import nlargest
from itertools import islice
from operator import attrgetter
from typing import TypeVar

from stream_processor.models import GROUP_PATTERN, Student, StudentRecord

R = TypeVar("R")


def parse_student(row: dict[str, str]) -> Student | str:
    raw_id = (row.get("student_id") or "").strip()
    if not (raw_id.isascii() and raw_id.isdigit()) or int(raw_id) < 1:
        return "bad_id"
    first_name = (row.get("first_name") or "").strip()
    last_name = (row.get("last_name") or "").strip()
    if not first_name or not last_name:
        return "empty_name"
    group = (row.get("group") or "").strip()
    if not GROUP_PATTERN.fullmatch(group):
        return "bad_group"
    try:
        grade = float(row["average_grade"])
    except (KeyError, TypeError, ValueError):
        return "bad_grade"
    if not math.isfinite(grade):
        return "bad_grade"
    if not 0 <= grade <= 100:
        return "grade_out_of_range"
    return Student(int(raw_id), first_name, last_name, group, round(grade, 2))


def validate_students(
    rows: Iterable[dict[str, str]],
    errors: Counter[str] | None = None,
) -> Iterator[Student]:
    for row in rows:
        result = parse_student(row)
        if isinstance(result, str):
            if errors is not None:
                errors[result] += 1
            continue
        yield result


def filter_by_group(students: Iterable[Student], group: str) -> Iterator[Student]:
    for student in students:
        if student.group == group:
            yield student


def filter_by_min_grade(students: Iterable[Student], minimum: float) -> Iterator[Student]:
    for student in students:
        if student.average_grade >= minimum:
            yield student


def find_first(records: Iterable[R], predicate: Callable[[R], bool]) -> R | None:
    return next((record for record in records if predicate(record)), None)


def find_by_id(records: Iterable[StudentRecord], student_id: int) -> StudentRecord | None:
    return find_first(records, lambda record: record.student_id == student_id)


def find_by_last_name(records: Iterable[StudentRecord], last_name: str) -> Iterator[StudentRecord]:
    wanted = last_name.casefold()
    for record in records:
        if record.last_name.casefold() == wanted:
            yield record


def first_n_students(
    records: Iterable[StudentRecord],
    predicate: Callable[[StudentRecord], bool],
    n: int,
) -> list[StudentRecord]:
    return list(islice((record for record in records if predicate(record)), n))


def top_students(
    records: Iterable[StudentRecord],
    n: int,
    predicate: Callable[[StudentRecord], bool] | None = None,
) -> list[StudentRecord]:
    candidates = records if predicate is None else (r for r in records if predicate(r))
    return nlargest(n, candidates, key=attrgetter("average_grade"))
