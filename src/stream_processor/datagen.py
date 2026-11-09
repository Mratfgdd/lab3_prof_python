import argparse
import csv
import random
from collections.abc import Iterator, Sequence
from itertools import count, islice
from pathlib import Path

from stream_processor.models import CSV_FIELDS

FIRST_NAMES = ("Олена", "Андрій", "Ірина", "Тарас", "Максим", "Софія", "Дмитро", "Катерина")
LAST_NAMES = ("Коваленко", "Мельник", "Бондар", "Гриценко", "Ткаченко", "Гнатюк", "Бойко")
GROUPS = ("PI-21", "PI-22", "PI-23", "PI-24", "PI-25")


def corrupt(row: list[str], rng: random.Random) -> list[str]:
    broken = row.copy()
    kind = rng.randrange(6)
    if kind == 0:
        broken[0] = "ID" + broken[0]
    elif kind == 1:
        broken[2] = ""
    elif kind == 2:
        broken[3] = "PI-2"
    elif kind == 3:
        broken[4] = broken[4].replace(".", "x")
    elif kind == 4:
        broken[4] = "100.5"
    else:
        broken = broken[:3]
    return broken


def random_students(seed: int = 42, invalid_share: float = 0.01) -> Iterator[list[str]]:
    rng = random.Random(seed)
    for student_id in count(1):
        row = [
            str(student_id),
            rng.choice(FIRST_NAMES),
            rng.choice(LAST_NAMES),
            rng.choice(GROUPS),
            f"{rng.uniform(40, 100):.2f}",
        ]
        yield corrupt(row, rng) if rng.random() < invalid_share else row


def write_rows(rows: Iterator[list[str]], path: Path, size: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_FIELDS)
        writer.writerows(islice(rows, size))
    return path


def generate_file(path: Path, size: int, seed: int = 42) -> Path:
    return write_rows(random_students(seed), path, size)


def generate_journal(root: Path, sizes: Sequence[int], seed: int = 7) -> list[Path]:
    stream = random_students(seed)
    paths = [root / f"2026-{10 + i // 2}" / f"part-{i + 1}.csv" for i in range(len(sizes))]
    return [write_rows(stream, path, size) for path, size in zip(paths, sizes, strict=True)]


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Генерація CSV зі студентами")
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path("data/generated/students.csv"))
    args = parser.parse_args(argv)
    path = generate_file(args.out, args.count, args.seed)
    print(f"Згенеровано {args.count} записів: {path}")


if __name__ == "__main__":
    main()
