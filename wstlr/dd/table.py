"""

"""

from wstlr import die_if 
from wstlr.dd.variable import DdVariable

class DdTable:
    def __init__(self, name, description=""):
        self.name = name
        self.description = description

        self.variables = {}
        self.key = []

    def add_variable(self, **kwargs):
        var = DdVariable(**kwargs)

        die_if(var.varname in self.variables, 
                f"{var.varname} appears more than once in definition for "
                f"table, {self.name}")
        
        self.variables[var.varname] = var
        if var.key_component:
            self.key.append(var.varname)