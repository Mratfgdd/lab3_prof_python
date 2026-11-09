from collections import Counter
from itertools import islice, takewhile
from pathlib import Path

from stream_processor.analytics import (
    count_by_group,
    min_max_grade,
    running_average,
    streaming_average,
    summarize,
)
from stream_processor.batches import Batcher, batch_statistics, batched_by_group
from stream_processor.datagen import random_students
from stream_processor.filters import find_by_id, find_by_last_name, first_n_students, top_students
from stream_processor.models import LEVEL_LABELS, StudentRecord
from stream_processor.pipeline import build_pipeline, export_csv
from stream_processor.readers import JournalSource

DATA = Path("data")
INPUT = DATA / "input.csv"
JOURNAL = DATA / "journal"
EXPORT = Path("results") / "excellent_students.csv"


def print_records(title: str, records: list[StudentRecord]) -> None:
    print(f"\n{title}")
    for r in records:
        print(
            f"  {r.student_id:3} {r.full_name:22} {r.group:6} "
            f"{r.average_grade:6.2f}  {LEVEL_LABELS[r.level]}"
        )


def ids(records: list[StudentRecord]) -> list[int]:
    return [record.student_id for record in records]


def show_stages() -> None:
    errors: Counter[str] = Counter()
    stages: Counter[str] = Counter()
    records = list(build_pipeline(INPUT, errors=errors, stages=stages))
    print("Етапи конвеєра (скільки записів пройшло):")
    print("  " + ", ".join(f"{name}={value}" for name, value in sorted(stages.items())))
    print(f"Відхилено під час validation: {errors.total()} {dict(errors)}")
    print_records("Оброблені студенти (input.csv):", records)


def show_filters() -> None:
    group = "PI-22"
    print_records(f"Фільтр за групою {group}:", list(build_pipeline(INPUT, group=group)))
    best = build_pipeline(INPUT, min_grade=90)
    print(f"\nФільтр average_grade >= 90: {[r.student_id for r in best]}")
    both = build_pipeline(INPUT, group="PI-21", min_grade=85)
    print(f"Група PI-21 і average_grade >= 85: {[r.full_name for r in both]}")


def show_search() -> None:
    stages: Counter[str] = Counter()
    found = find_by_id(build_pipeline(INPUT, stages=stages), 3)
    if found:
        print(
            f"\nПошук за ID 3: {found.full_name}, {found.group}; "
            f"прочитано рядків: {stages['1 read']} (early termination)"
        )
    matches = [r.full_name for r in find_by_last_name(build_pipeline(INPUT), "мельник")]
    print(f"Пошук за прізвищем 'мельник' (lazy generator): {matches}")
    stages = Counter()
    first = next(find_by_last_name(build_pipeline(INPUT, stages=stages), "Мельник"), None)
    if first:
        print(f"  перший збіг {first.full_name} знайдено після {stages['1 read']} рядків")


def show_aggregation() -> None:
    s = summarize(build_pipeline(INPUT))
    print("\nАгрегація (summarize, один прохід):")
    print(f"  студентів {s.records}, середній бал {s.average:.2f}")
    print(f"  мінімум {s.minimum:.2f}, максимум {s.maximum:.2f}, найкращий: {s.best_student}")
    print(f"  за групами: {dict(s.by_group)}")
    print(f"  середній бал за групами: {s.group_averages}")
    grades = (r.average_grade for r in build_pipeline(INPUT))
    print(f"streaming_average без list: {streaming_average(grades):.2f}")
    extremes = min_max_grade(build_pipeline(INPUT))
    if extremes:
        low, high = extremes
        print(f"min_max_grade: мінімум {low.full_name} {low.average_grade},")
        print(f"               максимум {high.full_name} {high.average_grade}")
    print(f"count_by_group (Counter): {dict(count_by_group(build_pipeline(INPUT)))}")
    running = list(running_average(build_pipeline(INPUT)))
    print(f"Накопичене середнє (accumulate): {running[:3]} ... {running[-1]}")
    passing = sum(1 for _ in takewhile(lambda r: r.average_grade >= 60, build_pipeline(INPUT)))
    print(f"takewhile: студентів до першого з балом < 60 - {passing}")


def show_top() -> None:
    top = top_students(build_pipeline(INPUT), 3)
    print(f"\nТоп-3 за балом (heapq.nlargest, пам'ять O(N)): {[r.full_name for r in top]}")
    in_group = top_students(build_pipeline(INPUT), 2, lambda r: r.group == "PI-21")
    print(f"Топ-2 у групі PI-21: {[r.full_name for r in in_group]}")
    first = first_n_students(build_pipeline(INPUT), lambda r: r.average_grade >= 85, 4)
    print(f"Перші 4 студенти з балом >= 85 (islice): {ids(first)}")


def show_batches() -> None:
    print("\nBatch processing (Batcher, size=5) і статистика пакета (groupby):")
    for number, batch in enumerate(Batcher(build_pipeline(INPUT), 5), 1):
        print(f"  пакет {number}: студентів {len(batch)}, {batch_statistics(batch)}")
    sizes = [len(batch) for batch in batched_by_group(build_pipeline(INPUT), max_size=4)]
    print(f"Динамічні пакети за групою (max_size=4): розміри {sizes}")


def show_journal() -> None:
    source = JournalSource(JOURNAL)
    files = list(source)
    print(
        f"\nЖурнал із кількох файлів (рекурсивний yield from): {len(files)} файли, "
        f"повторний обхід дає {len(list(source))}"
    )
    errors: Counter[str] = Counter()
    summary = summarize(build_pipeline(source, errors=errors))
    print(
        f"  chain: {summary.records} студентів, середній бал {summary.average:.2f}, "
        f"некоректних рядків {errors.total()}"
    )
    print(f"  за групами: {dict(sorted(summary.by_group.items()))}")


def show_infinite_and_export() -> None:
    print("\nНескінченний генератор + islice(..., 2):")
    for row in islice(random_students(seed=1), 2):
        print("  " + ",".join(row))
    excellent = (r for r in build_pipeline(INPUT) if r.level == "excellent")
    print(f"Streaming export відмінників: {export_csv(excellent, EXPORT)} -> {EXPORT}")


def main() -> None:
    print("ЛР3, варіант 1: потокова обробка даних системи обліку студентів\n")
    show_stages()
    show_filters()
    show_search()
    show_aggregation()
    show_top()
    show_batches()
    show_journal()
    show_infinite_and_export()


if __name__ == "__main__":
    main()
