"""CSV reader (SOURCE 1 customers, SOURCE 3 orders/items/payments).

All columns are read as strings; type casting happens in silver where failures can be
quarantined row-by-row instead of aborting the whole file.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv

from .base import ReadResult, SourceError


class CsvSource:
    format = "csv"

    def __init__(self, delimiter: str = ","):
        self.delimiter = delimiter

    def header(self, path: Path) -> list[str]:
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            try:
                return next(reader)
            except StopIteration as e:
                raise SourceError(f"{path} is empty (no header row)") from e

    def read(self, path: Path) -> ReadResult:
        if not path.exists():
            raise SourceError(f"source file not found: {path}")
        cols = self.header(path)
        if len(set(cols)) != len(cols):
            raise SourceError(f"{path} has duplicate header names: {cols}")
        try:
            table = pacsv.read_csv(
                path,
                parse_options=pacsv.ParseOptions(delimiter=self.delimiter, newlines_in_values=True),
                convert_options=pacsv.ConvertOptions(
                    column_types={c: pa.string() for c in cols},
                    strings_can_be_null=True,
                    null_values=["", "NULL", "null", "NaN", "nan"],
                ),
            )
        except pa.ArrowInvalid as e:
            raise SourceError(f"malformed CSV {path}: {e}") from e
        return ReadResult(table=table, stats={"columns": cols, "bytes": path.stat().st_size})
