import csv
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from stream_processor.analytics import (
    count_by_group,
    min_max_grade,
    running_average,
    streaming_average,
    summarize,
)
from stream_processor.datagen import generate_file, generate_journal
from stream_processor.experiment import eager_process, peak_memory
from stream_processor.filters import validate_students
from stream_processor.pipeline import build_pipeline, export_csv, read_rows
from stream_processor.readers import JournalSource

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "input.csv"


class SummaryTests(unittest.TestCase):
    def test_demo_summary_matches_manual_calculation(self):
        summary = summarize(build_pipeline(INPUT))
        self.assertEqual(summary.records, 19)
        self.assertAlmostEqual(summary.average, 80.91, places=2)
        self.assertEqual((summary.minimum, summary.maximum), (54.3, 98.4))
        self.assertEqual(summary.best_student, "Ігор Романюк")
        self.assertEqual(summary.by_group, {"PI-21": 6, "PI-22": 5, "PI-23": 5, "PI-24": 3})
        self.assertEqual(
            summary.group_averages, {"PI-21": 79.83, "PI-22": 80.18, "PI-23": 77.0, "PI-24": 90.8}
        )

    def test_streaming_functions_agree_with_summary(self):
        summary = summarize(build_pipeline(INPUT))
        grades = (r.average_grade for r in build_pipeline(INPUT))
        self.assertEqual(streaming_average(grades), summary.average)
        low, high = min_max_grade(build_pipeline(INPUT))
        self.assertEqual(
            (low.average_grade, high.average_grade), (summary.minimum, summary.maximum)
        )
        self.assertEqual(count_by_group(build_pipeline(INPUT)), summary.by_group)

    def test_running_average_ends_with_overall_average(self):
        values = list(running_average(build_pipeline(INPUT)))
        self.assertEqual(len(values), 19)
        self.assertEqual(values[:3], [91.5, 87.75, 88.07])
        self.assertEqual(values[-1], summarize(build_pipeline(INPUT)).average)

    def test_empty_stream(self):
        summary = summarize(iter(()))
        self.assertEqual((summary.records, summary.average, summary.minimum), (0, 0.0, 0.0))
        self.assertIsNone(summary.best_student)
        self.assertEqual(streaming_average(iter(())), 0.0)
        self.assertIsNone(min_max_grade(iter(())))
        self.assertEqual(list(running_average(iter(()))), [])
        self.assertEqual(count_by_group(iter(())), Counter())

    def test_single_student(self):
        records = list(build_pipeline(INPUT, group="PI-24", min_grade=97))
        low, high = min_max_grade(records)
        self.assertIs(low, high)
        self.assertEqual(summarize(records).average, 97.0)


class GeneratedDataTests(unittest.TestCase):
    def test_generated_file_eager_equals_lazy(self):
        with tempfile.TemporaryDirectory() as folder:
            path = generate_file(Path(folder) / "students.csv", 3_000, seed=5)
            lazy = summarize(build_pipeline(path))
            eager = summarize(eager_process(path))
            self.assertEqual(lazy, eager)
            students = list(validate_students(read_rows(path)))
            errors = Counter()
            list(build_pipeline(path, errors=errors))
        self.assertEqual(len(students), lazy.records)
        self.assertEqual(lazy.records + errors.total(), 3_000)
        self.assertGreater(lazy.records, 2_900)
        self.assertTrue(all(0 <= s.average_grade <= 100 for s in students))

    def test_journal_parts_continue_one_stream(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = generate_journal(Path(folder), [50, 50, 50])
            self.assertEqual(len(paths), 3)
            summary = summarize(build_pipeline(JournalSource(Path(folder))))
            self.assertGreater(summary.records, 140)

    def test_export_round_trip(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "out.csv"
            self.assertEqual(export_csv(build_pipeline(INPUT), path), 19)
            with path.open(encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
        self.assertEqual(len(rows), 19)
        self.assertEqual(rows[0]["level"], "excellent")


class MemoryTests(unittest.TestCase):
    def test_lazy_uses_less_memory_than_eager(self):
        with tempfile.TemporaryDirectory() as folder:
            path = generate_file(Path(folder) / "students.csv", 20_000, seed=9)
            eager = peak_memory(lambda: summarize(eager_process(path)))
            lazy = peak_memory(lambda: summarize(build_pipeline(path)))
        self.assertLess(lazy * 10, eager)


if __name__ == "__main__":
    unittest.main()
