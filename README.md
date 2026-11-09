# Student Stream Processor

Лабораторна робота №3 з дисципліни «Професійний Python»:
«Ітератори, генератори та потокова обробка даних».
Варіант 1: потокова обробка даних системи обліку студентів (продовження ЛР1-ЛР2).

## Опис

Lazy-конвеєр обробки CSV-журналу студентів
`student_id,first_name,last_name,group,average_grade`:

```
source -> read_lines -> clean_lines -> parse_rows -> validate_students
       -> filter_by_group -> filter_by_min_grade -> enrich_students -> aggregation / batching
```

Жоден рядок не читається, доки результат не споживають. Реалізовано:

- streaming CSV reading та validation (причини відхилення рахуються в `Counter`);
- фільтрація за групою та мінімальним середнім балом;
- пошук за ID і за прізвищем з early termination;
- streaming average, min/max та підрахунок студентів за групами без повної матеріалізації;
- перші N студентів із найвищим балом за умовою (`heapq.nlargest`) і перші N за умовою (`islice`);
- batch processing: власний iterator `Batcher`, динамічні пакети за групою, статистика пакета;
- рекурсивний пошук CSV у підкаталогах (`yield from`), `chain.from_iterable`, `accumulate`,
  `groupby`, `takewhile`, нескінченний генератор + `islice`, streaming export;
- експеримент eager vs lazy: час, піковий обсяг пам'яті, time to first result, early termination.

## Requirements

Python 3.11+. Зовнішніх залежностей немає.

## Installation

```
python -m venv .venv
.venv\Scripts\activate
python -m pip install -e .
```

## Run

```
python -m stream_processor.main
python -m stream_processor.datagen --count 100000 --out data/generated/students.csv
python -m stream_processor.experiment
```

або console commands `student-stream`, `student-stream-generate`, `student-stream-experiment`.
Експеримент створює набори в `data/generated/` (не додаються до Git) і зберігає результати в
`results/`.

## Tests

```
python -m unittest discover -s tests -v
```

(тести також запускаються через `pytest`).

## Project structure

```
data/input.csv                      демонстраційний журнал студентів
data/journal/                       той самий потік, розрізаний на 3 файли в підкаталогах
src/stream_processor/models.py      Student, StudentRecord, рівні успішності
src/stream_processor/readers.py     read_lines, iter_csv_files, JournalSource
src/stream_processor/parsers.py     clean_lines, parse_rows
src/stream_processor/filters.py     validation, фільтри, пошук, топ-N
src/stream_processor/transformations.py   enrich_students
src/stream_processor/batches.py     Batcher, batched_by_group, batch_statistics
src/stream_processor/analytics.py   summarize, streaming_average, min_max_grade, running_average
src/stream_processor/pipeline.py    build_pipeline, log_stage, export_csv
src/stream_processor/datagen.py     генерація тестових даних
src/stream_processor/experiment.py  eager vs lazy
src/stream_processor/main.py        точка входу
tests/                              модульні тести (unittest)
```

## Author

Student: Левандівський Назар
