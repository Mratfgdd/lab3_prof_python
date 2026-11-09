import argparse
import csv
import gc
import tracemalloc
from collections.abc import Callable, Sequence
from pathlib import Path
from time import perf_counter
from typing import NamedTuple, TypeVar

from stream_processor.analytics import summarize
from stream_processor.datagen import generate_file
from stream_processor.filters import find_first, parse_student
from stream_processor.models import Student, StudentRecord
from stream_processor.pipeline import build_pipeline
from stream_processor.transformations import grade_level

R = TypeVar("R")

SIZES: tuple[int, ...] = (10_000, 100_000, 500_000)
EARLY_SIZE = 1_000_000
DATA_DIR = Path("data") / "generated"


def eager_process(path: Path) -> list[StudentRecord]:
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    cleaned = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
    rows = list(csv.DictReader(cleaned))
    parsed = [parse_student(row) for row in rows]
    students = [item for item in parsed if isinstance(item, Student)]
    return [StudentRecord(*s, level=grade_level(s.average_grade)) for s in students]


def is_top_student(record: StudentRecord) -> bool:
    return record.group == "PI-25" and record.average_grade >= 99.97


def best_time(func: Callable[[], R], repeat: int) -> tuple[R, float]:
    best = float("inf")
    result: R
    for _ in range(repeat):
        gc.collect()
        start = perf_counter()
        result = func()
        best = min(best, perf_counter() - start)
    return result, best


def peak_memory(func: Callable[[], object]) -> int:
    gc.collect()
    tracemalloc.start()
    try:
        func()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return peak


class SizeResult(NamedTuple):
    records: int
    eager_time: float
    lazy_time: float
    eager_peak: int
    lazy_peak: int
    eager_first: float
    lazy_first: float


class EarlyResult(NamedTuple):
    records: int
    position: int
    eager_time: float
    lazy_time: float
    eager_peak: int
    lazy_peak: int


def dataset(size: int) -> Path:
    path = DATA_DIR / f"students_{size}.csv"
    if not path.exists():
        generate_file(path, size)
    return path


def measure_size(size: int) -> SizeResult:
    path = dataset(size)
    repeat = 3 if size <= 100_000 else 2
    eager, eager_time = best_time(lambda: summarize(eager_process(path)), repeat)
    lazy, lazy_time = best_time(lambda: summarize(build_pipeline(path)), repeat)
    assert eager == lazy, "eager і lazy дали різні результати"
    _, eager_first = best_time(lambda: eager_process(path)[0], repeat)
    _, lazy_first = best_time(lambda: next(build_pipeline(path)), repeat)
    return SizeResult(
        records=size,
        eager_time=eager_time,
        lazy_time=lazy_time,
        eager_peak=peak_memory(lambda: summarize(eager_process(path))),
        lazy_peak=peak_memory(lambda: summarize(build_pipeline(path))),
        eager_first=eager_first,
        lazy_first=lazy_first,
    )


def measure_early(size: int = EARLY_SIZE) -> EarlyResult:
    path = dataset(size)
    lazy, lazy_time = best_time(lambda: find_first(build_pipeline(path), is_top_student), 3)
    eager, eager_time = best_time(
        lambda: [r for r in eager_process(path) if is_top_student(r)][0], 1
    )
    assert lazy == eager
    position = next(
        index for index, record in enumerate(build_pipeline(path), 1) if is_top_student(record)
    )
    return EarlyResult(
        records=size,
        position=position,
        eager_time=eager_time,
        lazy_time=lazy_time,
        eager_peak=peak_memory(lambda: [r for r in eager_process(path) if is_top_student(r)][0]),
        lazy_peak=peak_memory(lambda: find_first(build_pipeline(path), is_top_student)),
    )


def save_csv(rows: Sequence[NamedTuple], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(rows[0]._fields)
        writer.writerows(rows)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Eager vs lazy")
    parser.add_argument("--sizes", type=int, nargs="+", default=list(SIZES))
    parser.add_argument("--early", type=int, default=EARLY_SIZE)
    parser.add_argument("--out", type=Path, default=Path("results"))
    args = parser.parse_args(argv)

    mb = 1024 * 1024
    print(
        f"{'Записів':>8} | {'eager, с':>8} | {'lazy, с':>7} | {'eager, МБ':>9} | "
        f"{'lazy, МБ':>8} | {'1-й eager, с':>12} | {'1-й lazy, мс':>12}"
    )
    sizes = []
    for size in args.sizes:
        row = measure_size(size)
        sizes.append(row)
        print(
            f"{row.records:>8} | {row.eager_time:>8.3f} | {row.lazy_time:>7.3f} | "
            f"{row.eager_peak / mb:>9.1f} | {row.lazy_peak / mb:>8.2f} | "
            f"{row.eager_first:>12.3f} | {row.lazy_first * 1e3:>12.3f}"
        )
    save_csv(sizes, args.out / "experiment.csv")

    early = measure_early(args.early)
    print(f"\nEarly termination ({early.records} записів, збіг на позиції {early.position}):")
    print(f"  eager: {early.eager_time:.3f} с, {early.eager_peak / mb:.1f} МБ")
    print(f"  lazy:  {early.lazy_time * 1e3:.3f} мс, {early.lazy_peak / 1024:.1f} КБ")
    save_csv([early], args.out / "early_termination.csv")


if __name__ == "__main__":
    main()
