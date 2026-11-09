import tempfile
import unittest
from collections import Counter
from itertools import islice
from pathlib import Path

from stream_processor.batches import Batcher, batch_statistics, batched_by_group
from stream_processor.datagen import random_students
from stream_processor.filters import (
    filter_by_group,
    filter_by_min_grade,
    find_by_id,
    find_by_last_name,
    find_first,
    first_n_students,
    parse_student,
    top_students,
)
from stream_processor.models import Student, StudentRecord
from stream_processor.parsers import clean_lines, parse_rows
from stream_processor.pipeline import build_pipeline
from stream_processor.readers import JournalSource, iter_csv_files, read_lines
from stream_processor.transformations import enrich_students, grade_level

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "input.csv"


def student(student_id=1, last_name="Коваль", group="PI-21", grade=80.0):
    return Student(student_id, "Анна", last_name, group, grade)


def record(student_id=1, last_name="Коваль", group="PI-21", grade=80.0):
    return next(enrich_students([student(student_id, last_name, group, grade)]))


def ids(items):
    return [item.student_id for item in items]


class ReaderTests(unittest.TestCase):
    def test_read_lines_is_lazy_generator(self):
        lines = read_lines(INPUT)
        self.assertIs(iter(lines), lines)
        self.assertTrue(next(lines).startswith("#"))
        lines.close()

    def test_bom_is_removed(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bom.csv"
            path.write_text("student_id,first_name\n1,A\n", encoding="utf-8-sig")
            rows = list(parse_rows(clean_lines(read_lines(path))))
        self.assertEqual(rows, [{"student_id": "1", "first_name": "A"}])

    def test_clean_lines_skips_blank_and_comments(self):
        self.assertEqual(list(clean_lines(["# c\n", "  \n", " a,b \n"])), ["a,b"])

    def test_recursive_file_discovery_and_reiterable_source(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "a" / "b").mkdir(parents=True)
            for name in ("x.csv", "a/y.csv", "a/b/z.csv", "a/readme.txt"):
                (root / name).write_text("", encoding="utf-8")
            names = [path.name for path in iter_csv_files(root)]
            source = JournalSource(root)
            self.assertEqual(sorted(names), ["x.csv", "y.csv", "z.csv"])
            self.assertEqual(list(source), list(source))


class ValidationTests(unittest.TestCase):
    def test_reasons(self):
        good = {
            "student_id": "7",
            "first_name": "Анна",
            "last_name": "Коваль",
            "group": "PI-21",
            "average_grade": "91.5",
        }
        cases = {
            "bad_id": {"student_id": "X7"},
            "empty_name": {"first_name": " "},
            "bad_group": {"group": "PI-2"},
            "bad_grade": {"average_grade": "9x"},
            "grade_out_of_range": {"average_grade": "100.1"},
        }
        self.assertEqual(parse_student(good), Student(7, "Анна", "Коваль", "PI-21", 91.5))
        for reason, change in cases.items():
            with self.subTest(reason=reason):
                self.assertEqual(parse_student(good | change), reason)

    def test_boundaries_and_edge_values(self):
        good = {"student_id": "1", "first_name": "А", "last_name": "Б", "group": "PI-21"}
        for grade in ("0", "100", " 50.556 "):
            self.assertIsInstance(parse_student(good | {"average_grade": grade}), Student)
        self.assertEqual(parse_student(good | {"average_grade": "50.556"}).average_grade, 50.56)
        for grade, reason in (
            ("-0.01", "grade_out_of_range"),
            ("nan", "bad_grade"),
            ("inf", "bad_grade"),
            ("", "bad_grade"),
        ):
            self.assertEqual(parse_student(good | {"average_grade": grade}), reason)
        for bad_id in ("0", "-3", "1.5", "²", ""):
            self.assertEqual(
                parse_student(good | {"average_grade": "5", "student_id": bad_id}), "bad_id"
            )

    def test_short_row_is_rejected(self):
        self.assertEqual(
            parse_student({"student_id": "1", "first_name": "А", "last_name": "Б"}), "bad_group"
        )

    def test_demo_file_counts(self):
        errors = Counter()
        stages = Counter()
        records = list(build_pipeline(INPUT, errors=errors, stages=stages))
        self.assertEqual(len(records), 19)
        self.assertEqual(errors.total(), 6)
        self.assertEqual(
            dict(errors),
            {"bad_id": 1, "empty_name": 1, "bad_group": 2, "bad_grade": 1, "grade_out_of_range": 1},
        )
        self.assertEqual(stages["3 parse"], 25)
        self.assertEqual(stages["1 read"], 28)


class TransformationTests(unittest.TestCase):
    def test_grade_levels_at_boundaries(self):
        expected = {
            100: "excellent",
            90: "excellent",
            89.99: "good",
            75: "good",
            74.99: "satisfactory",
            60: "satisfactory",
            59.99: "failing",
            0: "failing",
        }
        for grade, level in expected.items():
            self.assertEqual(grade_level(grade), level, grade)

    def test_enrich_adds_level_and_full_name(self):
        result = record(grade=95.2)
        self.assertEqual((result.level, result.full_name), ("excellent", "Анна Коваль"))

    def test_generator_is_lazy(self):
        def exploding():
            yield student(1)
            raise AssertionError("другий елемент не повинен запитуватися")

        self.assertEqual(next(enrich_students(exploding())).student_id, 1)


class FilterAndSearchTests(unittest.TestCase):
    def test_group_and_grade_filters(self):
        students = [
            student(1, group="PI-21", grade=90),
            student(2, group="PI-22", grade=70),
            student(3, group="PI-22", grade=90),
        ]
        self.assertEqual([s.student_id for s in filter_by_group(students, "PI-22")], [2, 3])
        self.assertEqual([s.student_id for s in filter_by_min_grade(students, 90)], [1, 3])
        self.assertEqual(list(filter_by_group(students, "PI-99")), [])

    def test_pipeline_filters(self):
        self.assertEqual(ids(build_pipeline(INPUT, group="PI-22")), [3, 5, 7, 13, 19])
        self.assertEqual(ids(build_pipeline(INPUT, min_grade=90)), [1, 3, 19, 9, 12, 16])
        both = build_pipeline(INPUT, group="PI-21", min_grade=85)
        self.assertEqual(ids(both), [1, 4, 14])

    def test_search_by_id_stops_early(self):
        stages = Counter()
        found = find_by_id(build_pipeline(INPUT, stages=stages), 3)
        self.assertEqual(found.full_name, "Ірина Бондар")
        self.assertEqual(stages["1 read"], 11)
        self.assertIsNone(find_by_id(build_pipeline(INPUT), 999))

    def test_search_by_last_name_is_lazy_and_case_insensitive(self):
        self.assertEqual(ids(find_by_last_name(build_pipeline(INPUT), "мЕЛЬНИК")), [2, 10, 16])
        stages = Counter()
        first = next(find_by_last_name(build_pipeline(INPUT, stages=stages), "Мельник"))
        self.assertEqual(first.student_id, 2)
        self.assertEqual(stages["1 read"], 4)
        self.assertEqual(list(find_by_last_name(build_pipeline(INPUT), "Невідоме")), [])

    def test_find_first_returns_none_when_nothing_matches(self):
        self.assertIsNone(find_first([1, 2, 3], lambda value: value > 5))
        self.assertEqual(find_first([1, 2, 3], lambda value: value > 1), 2)

    def test_first_n_in_file_order(self):
        result = first_n_students(build_pipeline(INPUT), lambda r: r.average_grade >= 85, 4)
        self.assertEqual(ids(result), [1, 4, 14, 3])
        self.assertEqual(first_n_students([], lambda r: True, 3), [])

    def test_top_students_by_grade(self):
        self.assertEqual(ids(top_students(build_pipeline(INPUT), 3)), [19, 12, 3])
        in_group = top_students(build_pipeline(INPUT), 2, lambda r: r.group == "PI-21")
        self.assertEqual(ids(in_group), [1, 14])
        self.assertEqual(len(top_students(build_pipeline(INPUT), 100)), 19)

    def test_top_students_keeps_file_order_on_ties(self):
        tied = [record(1, grade=90), record(2, grade=90), record(3, grade=90)]
        self.assertEqual(ids(top_students(tied, 2)), [1, 2])


class BatchTests(unittest.TestCase):
    def test_batcher_protocol(self):
        batcher = Batcher(range(7), 3)
        self.assertIs(iter(batcher), batcher)
        self.assertEqual(list(batcher), [[0, 1, 2], [3, 4, 5], [6]])
        with self.assertRaises(StopIteration):
            next(batcher)
        with self.assertRaises(ValueError):
            Batcher([], 0)

    def test_batches_follow_group_changes_and_size_limit(self):
        records = [
            record(1, group="A-1"),
            record(2, group="A-1"),
            record(3, group="A-1"),
            record(4, group="B-2"),
            record(5, group="A-1"),
        ]
        sizes = [[r.student_id for r in batch] for batch in batched_by_group(records, 2)]
        self.assertEqual(sizes, [[1, 2], [3], [4], [5]])
        demo = [len(b) for b in batched_by_group(build_pipeline(INPUT), max_size=4)]
        self.assertEqual(demo, [4, 2, 4, 1, 4, 1, 3])

    def test_batch_statistics_groups_unsorted_batch(self):
        batch = [
            record(1, group="PI-22", grade=80),
            record(2, group="PI-21", grade=70),
            record(3, group="PI-22", grade=90),
        ]
        self.assertEqual(batch_statistics(batch), {"PI-21": (1, 70.0), "PI-22": (2, 85.0)})

    def test_islice_limits_infinite_generator(self):
        rows = list(islice(random_students(seed=3), 5))
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows, list(islice(random_students(seed=3), 5)))


class StudentRecordTests(unittest.TestCase):
    def test_record_fields(self):
        self.assertIsInstance(record(), StudentRecord)
        self.assertEqual(StudentRecord._fields[-1], "level")


if __name__ == "__main__":
    unittest.main()
