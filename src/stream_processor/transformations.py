from collections.abc import Iterable, Iterator

from stream_processor.models import GRADE_LEVELS, Student, StudentRecord


def grade_level(grade: float) -> str:
    return next(label for threshold, label in GRADE_LEVELS if grade >= threshold)


def enrich_students(students: Iterable[Student]) -> Iterator[StudentRecord]:
    for student in students:
        yield StudentRecord(*student, level=grade_level(student.average_grade))
