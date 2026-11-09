import csv
from collections import Counter
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from typing import TypeVar

from stream_processor.filters import filter_by_group, filter_by_min_grade, validate_students
from stream_processor.models import StudentRecord
from stream_processor.parsers import clean_lines, parse_rows
from stream_processor.readers import read_lines
from stream_processor.transformations import enrich_students

T = TypeVar("T")


def _counted(name: str, items: Iterable[T], stages: Counter[str]) -> Iterator[T]:
    for item in items:
        stages[name] += 1
        yield item


def log_stage(name: str, items: Iterable[T], stages: Counter[str] | None) -> Iterable[T]:
    return items if stages is None else _counted(name, items, stages)


def read_rows(path: Path, stages: Counter[str] | None = None) -> Iterator[dict[str, str]]:
    lines = log_stage("1 read", read_lines(path), stages)
    cleaned = log_stage("2 clean", clean_lines(lines), stages)
    yield from log_stage("3 parse", parse_rows(cleaned), stages)


def build_pipeline(
    paths: Path | Iterable[Path],
    *,
    group: str | None = None,
    min_grade: float | None = None,
    errors: Counter[str] | None = None,
    stages: Counter[str] | None = None,
) -> Iterator[StudentRecord]:
    files = [paths] if isinstance(paths, Path) else paths
    rows = chain.from_iterable(read_rows(path, stages) for path in files)
    students = log_stage("4 validate", validate_students(rows, errors), stages)
    if group is not None:
        students = log_stage("5 filter group", filter_by_group(students, group), stages)
    if min_grade is not None:
        students = log_stage("6 filter grade", filter_by_min_grade(students, min_grade), stages)
    return iter(log_stage("7 transform", enrich_students(students), stages))


def export_csv(records: Iterable[StudentRecord], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(StudentRecord._fields)
        for record in records:
            writer.writerow(record)
            count += 1
    return count
