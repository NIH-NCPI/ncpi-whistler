"""
Contains all tables (and variables)
"""

from wstlr.dd.table import DdTable
from wstlr import system_base, dd_system_url


import pdb

class DdStudy:
    def __init__(self, name, description="", url_base=system_base):
        self.name = name
        self.description = description 
        self.url_base = url_base
        #pdb.set_trace()
        self.url = dd_system_url(self.url_base, 
                                "CodeSystem", 
                                self.name, 
                                "DataSet", 
                                None)
        self.tables = {}

    def varname_lookup(self, table_name):
        lkup = {}

        if table_name in self.tables:
            self.tables[table_name].add_to_varname_lookup(lkup)
        return lkup

    def add_to_varname_lookup(self, lkup):
        for varname, table in self.tables.items():
            table.add_to_varname_lookup(lkup)

    def load_from_config(self, config):
        """Builds out the tables and variables according to the details """
        """from the configuration file. 
        
        config is the configuration object reflecting contents of the file"""

        if "anvil_data_model" in config:
            filename = config.anvil_data_model

    def add_table(self, name, description=""):
        self.tables[name] = DdTable(name, 
                                    self.name,
                                    description=description, 
                                    url_base=self.url_base)

    def add_variable(self, table_name, **kwargs):
        self.tables[table_name].add_variable(**kwargs)
        
    def table_as_dd(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name].obj_as_dd_table()

    def table_as_cs(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name].obj_as_cs()
    
    def variables_as_cs(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name].variables_as_cs()

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

        return obj

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

        return obj