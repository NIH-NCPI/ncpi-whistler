"""

"""

from wstlr import die_if, system_base, dd_system_url
from wstlr.dd.variable import DdVariable
import pdb

class DdTable:
    def __init__(self, name, study_name, description="", **kwargs): #
        self.url_base=kwargs.get('url_base', system_base)
        consent_group=kwargs.get('consent_group')
        
        self.name = name
        self.description = description

        self.study_name = study_name
        self.consent_group = consent_group
        self.study_component = self.study_name
        if self.consent_group is not None:
            self.study_component = f"{self.study_name}-{self.consent_group}"

        self.url = dd_system_url(self.url_base, 
                                "CodeSystem", 
                                self.study_component, 
                                self.name, 
                                self.name)

        self.variables = {}
        self.key = []
        self.subject_id = kwargs.get('subject_id')

    @property
    def desc(self):
        if self.description is not None and len(self.description.strip()) > 0:
            return self.description
        return self.name

    def add_variable(self, **kwargs):
        var = DdVariable(study_name=self.study_name, 
                            table_name=self.name, 
                            url_base=self.url_base,
                            **kwargs)

        die_if(var.varname in self.variables, 
                f"{var.varname} appears more than once in definition for "
                f"table, {self.name}")
        
        self.variables[var.varname] = var
        if var.key_component:
            self.key.append(var.varname)

    def obj_as_cs(self):
        obj = {

        }

    def obj_as_dd_variable(self):
        """Data dictionary variables do not dump their variable's values """
        """only variable name/desc"""

        values = []
        for var in self.variables:
            values.append({
                "code": var.varname,
                "description": var.desc
            })
        obj = {
            "varname": self.name,
            "desc": self.desc,
            "type": "DD-Table",
            "url": self.url,
            "values" : values
        }

    def obj_as_dd_table(self):
        """Data Dictionary tables list variable's content (but only as code/desc)"""

        variables = []
        for var in self.variables:
            variables.append(var.obj_as_dd_variable())

        obj = {
            "table_name": self.name,
            "url": self.url,
            "variables" : variables
        }
    
    def obj_as_cs(self):
        values = []
        for variable in self.variables:
            values.append({
                "code": variable.varname,
                "description": variable.desc
            })

        obj = {
            "varname": self.table_name,
            "url": self.url,
            "study": self.study_name,
            "values": values
        }

        return obj

    def as_obj(self, deep_export=False):
        obj = {
            "table_name": self.name,
            "study": self.study_name,
            "url": self.url, 
            "variables": []
        }

        variables = []
        for var in self.variables:
            variables.append({
                "code": var.varname,
                "description": var.desc
            })

            if deep_export:
                variables[-1]['values'] = var.values_for_json()
        if self.consent_group is not None:
            obj['consent_group'] = self.consent_group