from collections.abc import Iterator
from pathlib import Path


def read_lines(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        yield from file


def iter_csv_files(root: Path) -> Iterator[Path]:
    for entry in sorted(root.iterdir()):
        if entry.is_dir():
            yield from iter_csv_files(entry)
        elif entry.suffix == ".csv":
            yield entry


class JournalSource:
    def __init__(self, root: Path) -> None:
        self.root = root

    def __iter__(self) -> Iterator[Path]:
        return iter_csv_files(self.root)
