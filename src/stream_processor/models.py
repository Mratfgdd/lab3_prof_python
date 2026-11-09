import re
from typing import NamedTuple

GROUP_PATTERN = re.compile(r"[A-Z]{2}-\d{2}")

CSV_FIELDS: tuple[str, ...] = ("student_id", "first_name", "last_name", "group", "average_grade")

GRADE_LEVELS: tuple[tuple[float, str], ...] = (
    (90.0, "excellent"),
    (75.0, "good"),
    (60.0, "satisfactory"),
    (0.0, "failing"),
)

LEVEL_LABELS: dict[str, str] = {
    "excellent": "відмінно",
    "good": "добре",
    "satisfactory": "задовільно",
    "failing": "незадовільно",
}


class Student(NamedTuple):
    student_id: int
    first_name: str
    last_name: str
    group: str
    average_grade: float


class StudentRecord(NamedTuple):
    student_id: int
    first_name: str
    last_name: str
    group: str
    average_grade: float
    level: str

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"
