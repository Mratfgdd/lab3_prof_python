import csv
from collections.abc import Iterable, Iterator


def clean_lines(lines: Iterable[str]) -> Iterator[str]:
    for line in lines:
        value = line.strip()
        if value and not value.startswith("#"):
            yield value


def parse_rows(lines: Iterable[str]) -> Iterator[dict[str, str]]:
    yield from csv.DictReader(lines)
