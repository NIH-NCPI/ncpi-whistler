"""
 * Provide ability to embed rows from one table as members of a different table
 * based on a specified common ID column
"""

from __future__ import annotations

import collections
import os
from csv import DictReader

from wstlr import fix_fieldname


class EmbedableTable:
    def __init__(self, table_name: str, target_table: str, join_column: str) -> None:
        self.table_name = table_name
        self.target = target_table
        self.join_col = fix_fieldname(join_column)

        # There can be more than one matching row per ID
        self.rows: dict[str, list[dict[str, str]]] = collections.defaultdict(list)
        self.column_names: list[str] = []

    def load_data(self, filename: str | os.PathLike[str]) -> None:
        with open(filename, "rt", encoding="utf-8-sig") as f:
            reader = DictReader(f, delimiter=",", quotechar='"')
            assert reader.fieldnames is not None
            fieldnames = [fix_fieldname(x) for x in reader.fieldnames]
            reader.fieldnames = fieldnames
            self.column_names = fieldnames

            if self.join_col not in self.column_names:
                print(
                    f"There was an error loading data from {filename}:\n"
                    + f"\tUnable to join on column name: '{self.join_col}' \n\tColumn not present in: \n\t\t* '"
                    + "'\n\t\t* '".join(self.column_names)
                    + "'"
                )
            assert self.join_col in self.column_names

            for line in reader:
                id = line[self.join_col]
                self.rows[id].append(line)

    def get_rows(self, id: str) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        if id in self.rows:
            for row in self.rows[id]:
                rows.append({"table_name": self.table_name})
                rows[-1].update(row)

        return rows
