import argparse
import ast
import csv
import html
import inspect
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "report"
PDF_PATH = ROOT / "Звіт_ЛР3_Левандівський.pdf"
EDGE = Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")

sys.path.insert(0, str(ROOT / "src"))

from stream_processor import (
    analytics,
    batches,
    experiment,
    filters,
    pipeline,
    readers,
    transformations,
)
from stream_processor.analytics import summarize
from stream_processor.pipeline import build_pipeline

ENV = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONIOENCODING": "utf-8"}
INPUT = ROOT / "data" / "input.csv"


def esc(text: str) -> str:
    return html.escape(text, quote=False)


def num(value: float, digits: int) -> str:
    return f"{value:,.{digits}f}".replace(",", " ")


def code(source: str) -> str:
    text = source.strip()
    cls = "code keep" if text.count("\n") < 16 else "code"
    return f'<pre class="{cls}">{esc(text)}</pre>'


def strip_docstrings(source: str) -> str:
    tree = ast.parse(source)
    drop: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.ClassDef) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                if isinstance(first.value.value, str) and first.end_lineno is not None:
                    drop.update(range(first.lineno, first.end_lineno + 1))
    kept = [line for n, line in enumerate(source.splitlines(), 1) if n not in drop]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(kept))


def src(*objects: object) -> str:
    return code("\n\n".join(strip_docstrings(inspect.getsource(o)).rstrip() for o in objects))


def run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args, cwd=ROOT, env=ENV, capture_output=True, text=True, encoding="utf-8", check=False
    )


def times(value: float) -> str:
    number = round(value)
    if number % 10 == 1 and number % 100 != 11:
        return "раз"
    if 2 <= number % 10 <= 4 and not 12 <= number % 100 <= 14:
        return "рази"
    return "разів"


def table(headers: list[str], rows: list[list[str]], cls: str = "") -> str:
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows)
    return f'<table class="{cls}"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'


def keep_numbers_together(markup: str) -> str:
    pieces = re.split(r"(<pre.*?</pre>)", markup, flags=re.S)
    return "".join(
        piece if piece.startswith("<pre") else re.sub(r"(?<=\d) (?=\d{3}\b)", "\u00a0", piece)
        for piece in pieces
    )


class Numbering:
    def __init__(self) -> None:
        self.table = 0
        self.figure = 0

    def tcap(self, text: str) -> str:
        self.table += 1
        return f'<p class="tcap">Таблиця {self.table} – {text}</p>'

    def fcap(self, text: str) -> str:
        self.figure += 1
        return f'<p class="fcap">Рисунок {self.figure} – {text}</p>'


def program_output() -> str:
    result = run([sys.executable, "-m", "stream_processor.main"])
    assert result.returncode == 0, result.stderr
    return result.stdout


def unit_tests() -> str:
    result = run([sys.executable, "-m", "unittest", "discover", "-s", "tests"])
    assert result.returncode == 0, result.stderr
    ran = re.search(r"Ran (\d+) tests? in ([\d.]+)s", result.stderr)
    assert ran and result.stderr.rstrip().endswith("OK"), result.stderr
    return f"Ran {ran.group(1)} tests in {ran.group(2)}s — OK"


def linters(venv: Path | None) -> list[tuple[str, str]]:
    if venv is None:
        return []
    scripts = venv / "Scripts"
    checks = [
        ("ruff check src tests", [scripts / "ruff.exe", "check", "--no-cache", "src", "tests"]),
        (
            "ruff format --check src tests",
            [scripts / "ruff.exe", "format", "--check", "--no-cache", "src", "tests"],
        ),
        (
            "mypy src tests",
            [scripts / "mypy.exe", "--no-incremental", "--cache-dir=nul", "src", "tests"],
        ),
    ]
    results = []
    for title, args in checks:
        result = run([str(a) for a in args])
        assert result.returncode == 0, result.stdout + result.stderr
        lines = [ln for ln in result.stdout.splitlines() if ln and "note:" not in ln]
        results.append((title, lines[-1]))
    return results


def read_csv(name: str) -> list[dict[str, float]]:
    with (ROOT / "results" / name).open(encoding="utf-8") as file:
        return [{k: float(v) for k, v in row.items()} for row in csv.DictReader(file)]


def project_tree() -> str:
    pkg = sorted(p.name for p in (ROOT / "src" / "stream_processor").glob("*.py"))
    pkg.remove("__init__.py")
    pkg.insert(0, "__init__.py")
    tests = sorted(p.name for p in (ROOT / "tests").glob("test_*.py"))
    lines = [
        "Lab_03/",
        "├── pyproject.toml, README.md, .gitignore",
        "├── data/",
        "│   ├── input.csv           демонстраційний журнал студентів",
        "│   ├── journal/            журнал із 3 файлів у підкаталогах",
        "│   └── generated/          набори для експерименту (не в git)",
        "├── src/stream_processor/   " + ", ".join(pkg[:4]),
        "│                           " + ", ".join(pkg[4:8]),
        "│                           " + ", ".join(pkg[8:]),
        "├── tests/                  " + ", ".join(tests),
        "├── results/                experiment.csv, early_termination.csv, …",
        "└── report/build_report.py  генератор цього звіту",
    ]
    return "\n".join(lines)


PIPELINE_SCHEME = """
<div class="scheme">
  <div class="row">
    <div class="box"><b>Source</b><code>data/*.csv</code><code>JournalSource</code></div>
    <div class="arr">→</div>
    <div class="box"><b>Читання</b><code>read_lines</code></div>
    <div class="arr">→</div>
    <div class="box"><b>Очищення</b><code>clean_lines</code></div>
    <div class="arr">→</div>
    <div class="box"><b>Parsing</b><code>parse_rows</code></div>
    <div class="arr">→</div>
    <div class="box"><b>Validation</b><code>validate_students</code></div>
  </div>
  <div class="down">↓ chain.from_iterable об'єднує файли;
    log_stage рахує записи на кожному етапі</div>
  <div class="row">
    <div class="box"><b>Filtering</b><code>filter_by_group</code></div>
    <div class="arr">→</div>
    <div class="box"><b>Filtering</b><code>filter_by_min_grade</code></div>
    <div class="arr">→</div>
    <div class="box"><b>Transformation</b><code>enrich_students</code></div>
    <div class="arr">→</div>
    <div class="box wide"><b>Aggregation / batching / sink</b>
      <code>summarize · running_average · top_students</code>
      <code>Batcher · batch_statistics · export_csv</code></div>
  </div>
</div>"""


CSS = """
@page { size: A4; margin: 20mm 15mm 20mm 30mm;
  @bottom-center { content: counter(page); font: 12pt "Times New Roman", serif; } }
@page :first { @bottom-center { content: none; } }
html { background: #fff; }
body { margin: 0; color: #000; background: #fff; font-family: "Times New Roman", serif;
  font-size: 14pt; line-height: 1.22; }
p { margin: 0 0 2pt; text-indent: 1.25cm; text-align: justify; }
h1 { font-size: 14pt; text-align: center; margin: 10pt 0 5pt; break-after: avoid; }
h2 { font-size: 14pt; margin: 6pt 0 3pt; text-indent: 1.25cm; break-after: avoid; }
code { font-family: Consolas, "Courier New", monospace; font-size: 0.82em; }
pre.code, pre.out { font-family: Consolas, "Courier New", monospace; font-size: 8.8pt;
  line-height: 1.15; margin: 3pt 0 6pt; padding: 3pt 8pt; white-space: pre-wrap;
  background: #f6f6f4; border-left: 3px solid #c3c2b7; orphans: 4; widows: 4; }
pre.keep { break-inside: avoid; }
pre.out { font-size: 8.2pt; background: #fbfbfa; border: 1px solid #c3c2b7; }
table { width: 100%; border-collapse: collapse; font-size: 11pt; line-height: 1.15;
  margin: 2pt 0 6pt; }
th, td { border: 1px solid #000; padding: 2pt 5pt; vertical-align: top; text-align: left; }
th { background: #f0efec; }
table.num td { text-align: right; white-space: nowrap; }
table.num td:first-child { text-align: left; }
tr { break-inside: avoid; }
.tcap { text-indent: 0; text-align: left; margin: 5pt 0 2pt; break-after: avoid; }
.fcap { text-indent: 0; text-align: center; margin: 3pt 0 6pt; }
.scheme { font-size: 10pt; line-height: 1.2; margin: 4pt 0 0; break-inside: avoid; }
.scheme .row { display: flex; align-items: stretch; gap: 3pt; }
.scheme .box { flex: 1 1 0; border: 1.2px solid #2a78d6; border-radius: 4px; padding: 3pt 4pt;
  background: #f3f7fd; text-align: center; display: flex; flex-direction: column;
  justify-content: center; }
.scheme .box.wide { flex: 1.8 1 0; }
.scheme .box b { font-size: 9.5pt; }
.scheme .box code { font-size: 8.5pt; }
.scheme .arr { align-self: center; font-size: 13pt; color: #52514e; }
.scheme .down { text-align: center; color: #52514e; margin: 2pt 0; font-size: 9.5pt; }
.title { height: 255mm; display: flex; flex-direction: column; justify-content: space-between;
  text-align: center; break-after: page; line-height: 1.5; }
.title .main { margin-top: 30mm; }
.title .main .big { font-size: 20pt; font-weight: bold; letter-spacing: 2pt; }
.title .main .theme { font-size: 16pt; font-weight: bold; margin-top: 10pt; }
.title .sign { align-self: flex-end; text-align: left; width: 85mm; margin-top: 30mm; }
.title .city { margin-top: auto; }
"""


def speed_word(ratio: float) -> str:
    if ratio < 0.95:
        return "швидший"
    if ratio > 1.05:
        return "повільніший"
    return "практично однаковий за часом"


def build_html(venv: Path | None) -> str:
    n = Numbering()
    output = program_output()
    tests = unit_tests()
    lint = linters(venv)
    sizes = read_csv("experiment.csv")
    (early,) = read_csv("early_termination.csv")
    mb = 1024 * 1024

    records = list(build_pipeline(INPUT))
    s = summarize(iter(records))
    group_sums = {}
    for r in records:
        group_sums[r.group] = group_sums.get(r.group, 0.0) + r.average_grade
    total = sum(group_sums.values())
    assert f"{s.average:.2f}" in output and s.records == 19 and round(total / 19, 2) == s.average
    error_counter: Counter[str] = Counter()
    stage_total: Counter[str] = Counter()
    list(build_pipeline(INPUT, errors=error_counter, stages=stage_total))
    errors, stages = dict(error_counter), dict(stage_total)
    assert sum(errors.values()) == 6 and stages["3 parse"] == 25

    stage_counter: Counter[str] = Counter()
    found = filters.find_by_id(build_pipeline(INPUT, stages=stage_counter), 3)
    assert found is not None
    lines_read = stage_counter["1 read"]
    surnames = list(filters.find_by_last_name(build_pipeline(INPUT), "мельник"))
    top = filters.top_students(build_pipeline(INPUT), 3)
    extremes = analytics.min_max_grade(build_pipeline(INPUT))
    assert extremes is not None
    low, high = extremes
    group_pi22 = list(build_pipeline(INPUT, group="PI-22"))
    excellent = list(build_pipeline(INPUT, min_grade=90))
    dynamic = [len(b) for b in batches.batched_by_group(build_pipeline(INPUT), max_size=4)]
    fixed = [len(b) for b in batches.Batcher(build_pipeline(INPUT), 5)]
    assert f"прочитано рядків: {lines_read}" in output
    assert f"розміри {dynamic}" in output

    parts: list[str] = []
    add = parts.append

    add("""
<section class="title">
  <div>МІНІСТЕРСТВО ОСВІТИ І НАУКИ УКРАЇНИ<br>
  Львівський національний університет імені Івана Франка</div>
  <div class="main">
    <div class="big">ЗВІТ</div>
    <div>до лабораторної роботи №3</div>
    <div>з дисципліни «Професійний Python»</div>
    <div class="theme">Тема: «Ітератори, генератори та потокова обробка даних»</div>
  </div>
  <div class="sign">Виконав:<br>студент Левандівський Назар<br><br>
  Перевірив:<br>Куньо Іван Михайлович</div>
  <div class="city">Львів – 2026</div>
</section>""")

    add("<h1>Мета роботи</h1>")
    add(
        "<p>Сформувати практичні навички роботи з ітераційною моделлю Python: iterable та "
        "iterator, власні iterator classes, generator functions, yield, yield from, generator "
        "expressions, модуль itertools, lazy evaluation та потокова обробка великих наборів "
        "даних із порівнянням eager і lazy підходів за часом і пам'яттю.</p>"
    )

    add("<h1>1. Постановка задачі</h1>")
    add(
        "<p><b>Варіант 1. Потокова обробка даних системи обліку студентів</b> (узгоджено з "
        "ЛР1–ЛР2). Вхід – CSV-журнал студентів <code>student_id,first_name,last_name,group,"
        "average_grade</code>. Потрібно побудувати lazy-конвеєр source → reading → cleaning → "
        "parsing → validation → filtering → transformation → aggregation/batching, обробити "
        "не менше 100 000 записів, порівняти eager і lazy варіанти та оцінити пам'ять.</p>"
    )
    add(n.tcap("Вимоги варіанта 1 та їх реалізація (результат на data/input.csv)"))
    add(
        table(
            ["Вимога", "Реалізація", "Результат"],
            [
                [
                    "streaming CSV reading та validation студентів",
                    "<code>read_lines</code>, <code>validate_students</code>",
                    f"відсіяно {sum(errors.values())} рядків із {stages['3 parse']}",
                ],
                [
                    "фільтрація за групою та мін. середнім балом",
                    "<code>filter_by_group</code>, <code>filter_by_min_grade</code>",
                    f"PI-22: {len(group_pi22)} студентів; ≥ 90: {len(excellent)} студентів",
                ],
                [
                    "пошук за прізвищем або ID (lazy)",
                    "<code>find_by_last_name</code>, <code>find_by_id</code>",
                    f"ID 3 знайдено після {lines_read} рядків; «мельник» – {len(surnames)} студенти",
                ],
                [
                    "streaming average середнього бала",
                    "<code>streaming_average</code>, <code>summarize</code>",
                    f"{s.average:.2f}",
                ],
                [
                    "min/max без повної матеріалізації",
                    "<code>min_max_grade</code>",
                    f"{low.average_grade:.2f} ({esc(low.full_name)}) / "
                    f"{high.average_grade:.2f} ({esc(high.full_name)})",
                ],
                [
                    "перші N студентів із найвищим балом за умовою",
                    "<code>top_students</code> (heapq), <code>first_n_students</code> (islice)",
                    ", ".join(esc(r.full_name) for r in top),
                ],
                [
                    "batch processing записів",
                    "<code>Batcher</code>, <code>batched_by_group</code>, "
                    "<code>batch_statistics</code>",
                    f"{len(fixed)} пакети по ≤ 5; {len(dynamic)} динамічних за групою",
                ],
                [
                    "підрахунок студентів за групами",
                    "<code>count_by_group</code>, <code>summarize</code>",
                    ", ".join(f"{g} – {c}" for g, c in sorted(s.by_group.items())),
                ],
            ],
        )
    )

    add("<h1>2. Вхідні дані</h1>")
    add(
        "<p>Кожен рядок журналу – один студент. Правила validation: <code>student_id</code> – "
        "додатне ціле, ім'я та прізвище не порожні, <code>group</code> має формат "
        "<code>PI-21</code> (дві літери, дефіс, дві цифри), <code>average_grade</code> – "
        "скінченне число від 0 до 100. Причина кожного відхилення рахується в "
        "<code>Counter</code>: <code>bad_id</code>, <code>empty_name</code>, "
        "<code>bad_group</code>, <code>bad_grade</code>, <code>grade_out_of_range</code>.</p>"
    )
    add(
        f"<p>Демонстраційний файл <code>data/input.csv</code> містить коментар, порожній "
        f"рядок і {stages['3 parse']} записів, відсортованих за групами; серед них "
        f"{sum(errors.values())} навмисно некоректних. Для експерименту нескінченний генератор "
        "<code>random_students</code> створює журнал (≈1 % зіпсованих рядків), а "
        "<code>islice</code> обмежує його до 10 000 … 1 000 000 записів; "
        "<code>data/journal/</code> – той самий потік, розрізаний на 3 файли в "
        "підкаталогах.</p>"
    )

    add("<h1>3. Структура проєкту</h1>")
    add(f'<pre class="code keep">{esc(project_tree())}</pre>')

    add("<h1>4. Iterable та iterator</h1>")
    add(
        "<p>Iterable – об'єкт, з якого <code>iter()</code> отримує iterator; iterator зберігає "
        "стан обходу, віддає елементи через <code>__next__()</code> і завершується "
        "<code>StopIteration</code>. Цикл <code>for</code> викликає <code>iter()</code>, потім "
        "<code>next()</code> до <code>StopIteration</code>. Власний iterator "
        "<code>Batcher</code> віддає пакети студентів; він one-shot "
        "(<code>iter(b) is b</code>). <code>JournalSource</code> – окремий iterable: кожен "
        "<code>iter()</code> створює новий генератор файлів, тому його можна обходити "
        "повторно.</p>"
    )
    add(src(batches.Batcher, readers.JournalSource))

    add("<h1>5. Генератори, yield і yield from</h1>")
    add(
        "<p>Streaming reader і рекурсивний пошук файлів використовують <code>yield from</code>; "
        "файл відкритий, доки генератор не вичерпано або не закрито:</p>"
    )
    add(src(readers.read_lines, readers.iter_csv_files))
    add(
        "<p>Transformation stage – generator function з <code>yield</code>: кожен студент "
        "отримує поле <code>level</code> (рівень успішності) лише тоді, коли його запитує "
        "наступний етап. Етап не має власного стану, тому пам'ять не залежить від "
        "кількості студентів:</p>"
    )
    add(src(transformations.grade_level, transformations.enrich_students))
    add(
        "<p>Generator expressions використано у <code>build_pipeline</code> "
        "(<code>read_rows(path) for path in files</code>), <code>find_first</code>, "
        "<code>first_n_students</code>, <code>top_students</code> і "
        "<code>running_average</code>.</p>"
    )
    add(n.tcap("Використані засоби itertools"))
    add(
        table(
            ["Засіб", "Де використано", "Призначення"],
            [
                [
                    "<code>islice</code>",
                    "<code>Batcher</code>, <code>first_n_students</code>, datagen",
                    "пакети, перші N, обмеження нескінченного потоку",
                ],
                [
                    "<code>chain.from_iterable</code>",
                    "<code>build_pipeline</code>",
                    "один потік із кількох файлів",
                ],
                [
                    "<code>accumulate</code>",
                    "<code>running_average</code>",
                    "накопичене середнє після кожного студента",
                ],
                [
                    "<code>groupby</code>",
                    "<code>batched_by_group</code>, <code>batch_statistics</code>",
                    "пакети за групою; статистика пакета (після sorted)",
                ],
                ["<code>count</code>", "<code>random_students</code>", "ідентифікатори студентів"],
                [
                    "<code>takewhile</code>",
                    "<code>main</code>",
                    "студенти до першого з балом &lt; 60",
                ],
            ],
        )
    )

    add("<h1>6. Lazy pipeline</h1>")
    add(PIPELINE_SCHEME)
    add(n.fcap("Схема потокового конвеєра"))
    add(
        "<p>Функція <code>build_pipeline</code> лише з'єднує генератори – жоден рядок не "
        "читається, доки результат не почне споживати sink:</p>"
    )
    add(src(pipeline.build_pipeline))
    add(
        "<p>Validation, filtering, топ-N, streaming aggregation і динамічні пакети. "
        "<code>nlargest</code> тримає в пам'яті лише N кандидатів, а не весь потік:</p>"
    )
    add(
        src(
            filters.validate_students,
            filters.filter_by_min_grade,
            filters.top_students,
            analytics.streaming_average,
            analytics.running_average,
            batches.batched_by_group,
        )
    )

    add("<h1>7. Результат роботи програми</h1>")
    add("<p>Вивід <code>python -m stream_processor.main</code>:</p>")
    add(f'<pre class="out">{esc(output.rstrip())}</pre>')
    sums = " + ".join(num(group_sums[g], 1) for g in sorted(group_sums))
    add(
        f"<p>Ручна перевірка: суми балів за групами {sums} = {num(total, 1)}; "
        f"{num(total, 1)} / {s.records} = {s.average:.2f}, що збігається з "
        "<code>summarize</code> і <code>streaming_average</code>; жоден бал не виходить за "
        "межі 0–100.</p>"
    )

    add("<h1>8. Eager і lazy: експеримент</h1>")
    add(
        "<p>Eager-версія повністю матеріалізує кожен етап у list; lazy-версія – це "
        "<code>build_pipeline</code>. Обидві використовують ту саму функцію перевірки "
        "<code>parse_student</code> і дають однаковий Summary (перевіряється assert). Час – "
        "найкращий із 2–3 запусків без tracemalloc, пам'ять – пік tracemalloc в окремому "
        "запуску, sink – <code>summarize</code>.</p>"
    )
    add(src(experiment.eager_process))
    add(n.tcap("Elapsed time повної обробки"))
    add(
        table(
            ["Records", "Eager time, с", "Lazy time, с", "Lazy / eager"],
            [
                [
                    num(r["records"], 0),
                    num(r["eager_time"], 3),
                    num(r["lazy_time"], 3),
                    num(r["lazy_time"] / r["eager_time"], 2),
                ]
                for r in sizes
            ],
            cls="num",
        )
    )
    add(n.tcap("Peak memory і time to first result"))
    add(
        table(
            [
                "Records",
                "Eager peak, МБ",
                "Lazy peak, МБ",
                "Eager / lazy",
                "1-й результат eager, с",
                "1-й результат lazy, мс",
            ],
            [
                [
                    num(r["records"], 0),
                    num(r["eager_peak"] / mb, 1),
                    num(r["lazy_peak"] / mb, 2),
                    num(r["eager_peak"] / r["lazy_peak"], 0),
                    num(r["eager_first"], 3),
                    num(r["lazy_first"] * 1e3, 3),
                ]
                for r in sizes
            ],
            cls="num",
        )
    )
    add(
        n.tcap(
            f"Early termination: перший студент групи PI-25 з балом ≥ 99.97 "
            f"({num(early['records'], 0)} записів)"
        )
    )
    add(
        table(
            ["Варіант", "Час", "Peak memory"],
            [
                [
                    "eager (повний filtered list, потім [0])",
                    f"{num(early['eager_time'], 3)} с",
                    f"{num(early['eager_peak'] / mb, 1)} МБ",
                ],
                [
                    "lazy (<code>find_first</code>)",
                    f"{num(early['lazy_time'] * 1e3, 3)} мс",
                    f"{num(early['lazy_peak'] / 1024, 1)} КБ",
                ],
            ],
            cls="num",
        )
    )

    big = sizes[-1]
    small = sizes[0]
    small_ratio = small["lazy_time"] / small["eager_time"]
    big_ratio = big["lazy_time"] / big["eager_time"]
    memory_ratio = big["eager_peak"] / big["lazy_peak"]
    early_ratio = early["eager_time"] / early["lazy_time"]
    assert big["lazy_peak"] < 2 * mb and small["lazy_peak"] < 2 * mb
    assert memory_ratio > 50 and big["eager_peak"] > 10 * small["eager_peak"]
    assert big["lazy_first"] * 100 < big["eager_first"]
    assert early["position"] < early["records"] / 10 and early_ratio > 10
    add(
        f"<p><b>Аналіз.</b> Пікова пам'ять eager росте лінійно з кількістю записів "
        f"({num(small['eager_peak'] / mb, 1)} → {num(big['eager_peak'] / mb, 1)} МБ), а lazy "
        f"залишається ≈{num(big['lazy_peak'] / mb, 2)} МБ (буфер читання, поточний запис і "
        f"лічильники) – у {num(memory_ratio, 0)} {times(memory_ratio)} менше на "
        f"{num(big['records'], 0)} записів. За загальним часом на "
        f"{num(small['records'], 0)} записів lazy {speed_word(small_ratio)} "
        f"(lazy/eager = {num(small_ratio, 2)}), на {num(big['records'], 0)} записів – "
        f"{speed_word(big_ratio)} (lazy/eager = {num(big_ratio, 2)}): обидві версії виконують "
        f"ту саму обчислювальну роботу, тож різниця в часі невелика; найімовірніше, її "
        f"дають виділення й звільнення пам'яті під проміжні списки в eager-версії. Тому "
        f"висновок «generator завжди швидший» неправильний: його "
        f"головні переваги – пам'ять і перший результат, який lazy-конвеєр дає за "
        f"{num(big['lazy_first'] * 1e3, 2)} мс проти {num(big['eager_first'], 2)} с. Early "
        f"termination на {num(early['records'], 0)} записах знаходить збіг на позиції "
        f"{num(early['position'], 0)}, прочитавши лише початок файла, – у "
        f"{num(early_ratio, 0)} {times(early_ratio)} швидше і з "
        f"{num(early['lazy_peak'] / 1024, 0)} КБ пам'яті замість "
        f"{num(early['eager_peak'] / mb, 0)} МБ.</p>"
    )
    add(
        "<p>Недоліки lazy-підходу: конвеєр one-shot (для другого проходу його треба створити "
        "заново), немає <code>len()</code> та індексації, помилки даних виникають під час "
        "споживання, а файл залишається відкритим, доки генератор не завершено. Eager "
        "доцільний для невеликих наборів із повторним доступом і сортуванням; lazy – для "
        "великих файлів, потоків і пошуку з ранньою зупинкою.</p>"
    )

    add("<h1>9. Перевірка правильності</h1>")
    add(
        "<p>Тести (<code>tests/</code>, unittest) перевіряють протокол iterator, BOM, "
        "рекурсивний пошук файлів, кожну причину відхилення та граничні значення балу, "
        "лінивість генератора, early termination, топ-N із рівними балами, пакети, "
        "eager = lazy, узгодженість згенерованих даних, експорт і те, що lazy займає щонайменше "
        "в 10 разів менше пам'яті. Пакет встановлюється командою <code>pip install -e .</code> "
        "у чисте venv.</p>"
    )
    checks = [["<code>python -m unittest discover -s tests</code>", esc(tests)]]
    checks += [[f"<code>{esc(t)}</code>", esc(line)] for t, line in lint]
    add(n.tcap("Результати перевірок"))
    add(table(["Перевірка", "Результат"], checks))

    add("<h1>Висновки</h1>")
    add(
        "<p>Побудовано потоковий конвеєр обробки журналу студентів (варіант 1), у якому "
        "кожен запис проходить етапи читання, очищення, parsing, validation, filtering, "
        "transformation і aggregation лише тоді, коли його запитує споживач. Використано "
        "власний iterator, окремий iterable, generator functions, yield from (зокрема "
        "рекурсивний), generator expressions і шість засобів itertools, а також "
        "<code>heapq.nlargest</code> для топ-N. Експеримент показав, що lazy-обробка тримає "
        "пам'ять сталою і дає перший результат майже миттєво, тоді як eager потребує пам'яті, "
        "пропорційної розміру файла; за загальним часом різниця невелика й залежить від "
        "обсягу даних. Тож вибір між eager і lazy залежить від обсягу даних і способу їх "
        "використання.</p>"
    )

    body = keep_numbers_together("\n".join(parts))
    return (
        "<!doctype html><html lang='uk'><head><meta charset='utf-8'>"
        f"<title>Звіт ЛР3</title><style>{CSS}</style></head><body>{body}</body></html>"
    )


def print_pdf(html_path: Path, pdf_path: Path, profile: Path) -> None:
    args = [
        str(EDGE),
        "--headless=new",
        "--disable-gpu",
        "--no-pdf-header-footer",
        "--print-to-pdf-no-header",
        f"--user-data-dir={profile}",
        f"--print-to-pdf={pdf_path}",
        html_path.as_uri(),
    ]
    subprocess.run(args, check=True, capture_output=True, timeout=120)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--venv", type=Path, default=None)
    parser.add_argument(
        "--profile", type=Path, default=Path(tempfile.gettempdir()) / "lab3_report_edge_profile"
    )
    args = parser.parse_args()

    html_path = REPORT_DIR / "report.html"
    html_path.write_text(build_html(args.venv), encoding="utf-8")
    if PDF_PATH.exists():
        PDF_PATH.unlink()
    print_pdf(html_path, PDF_PATH, args.profile)

    import pymupdf

    with pymupdf.open(PDF_PATH) as document:
        print(f"PDF: {PDF_PATH} — {document.page_count} сторінок")


if __name__ == "__main__":
    main()
