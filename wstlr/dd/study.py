"""
Contains all tables (and variables)
"""

from wstlr.dd.table import DdTable
from wstlr import system_base

import pdb

class DdStudy:
    def __init__(self, name, description="", url_base=system_base):
        self.name = name
        self.description = description 
        self.url_base = url_base

        self.tables = {}

    def load_from_config(self, config):
        """Builds out the tables and variables according to the details """
        """from the configuration file. 
        
        config is the configuration object reflecting contents of the file"""

        if "anvil_data_model" in config:
            filename = config.anvil_data_model

    def add_table(self, name, description=""):
        self.tables[name] = DdTable(name, 
                                    description, 
                                    url_base=self.url_base)

    def add_variable(self, table_name, **kwargs):
        self.tables[table_name].add_variable(**kwargs)
        

    def obj_as_dd(self):
        values = []
        for table_name, table in self.tables.items():
            values.append(table.obj_as_dd_variable())
        obj = {
            "url": self.url,
            "study": self.name,
            "table_name": "DataSet",
            "values": values
        }

    def obj_as_cs(self):
        values = []
        for table_name, table in self.tables.items():
            values.append({
                "code": table_name,
                "description": table.desc
            })

        obj = {
            "url": self.url,
            "study": self.name,
            "table_name": "DataSet",
            "values": values
        }