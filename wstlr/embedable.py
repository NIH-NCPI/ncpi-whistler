"""
 * Provide ability to embed rows from one table as members of a different table
 * based on a specified common ID column
"""

import collections
from csv import DictReader
from wstlr import fix_fieldname

class EmbedableTable:
    def __init__(self, table_name, target_table, join_column):
        self.table_name = table_name
        self.target = target_table
        self.join_col = fix_fieldname(join_column)

        # There can be more than one matching row per ID 
        self.rows = collections.defaultdict(list)
        self.column_names = []

    def load_data(self, filename):
        with open(filename, 'rt') as f:
            reader = DictReader(f, delimiter=',', quotechar='"')
            reader.fieldnames = [fix_fieldname(x) for x in reader.fieldnames]
            self.column_names = reader.fieldnames

            if self.join_col not in self.column_names:
                print(f"Unable to join on column name: {self.join_col}. Column not present in: {self.column_names}")
            assert(self.join_col in self.column_names)

            for line in reader:
                id = line[self.join_col]
                self.rows[id].append(line)

    def get_rows(self, id):
        rows = []
        if id in self.rows:
            for row in self.rows[id]:
                rows.append({"table_name": self.table_name})
                rows[-1].update(row)

        return rows
