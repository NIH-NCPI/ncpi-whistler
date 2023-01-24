"""
Contains all tables (and variables)
"""

from wstlr.dd.table import DdTable

class DdStudy:
    def __init__(self, name, description=""):
        self.name = name
        self.description = description 

        self.tables = {}

    def add_table(self, name, description=""):
        self.tables[name] = DdTable(name, description)

    def add_variable(self, table_name, **kwargs):
        self.tables[table_name].add_variable(**kwargs)