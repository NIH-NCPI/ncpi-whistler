"""
Data Dictionary Variable

The variable represents a single column of data from a data table. 

TODO:
We'll need a mechanism to make the format of the incoming data flexible enough
to support different column headers or variable names for the same properties.

There must be a way to support more dynamic forms for data-dictionaries, such
as JSON objects or even some other format, so the incoming data should 


When using dictreader, each row is just a dictionary. When using the json 
object to represent a variable, the variable is a dictionary. So, ultimately,
we need a simple class to iterate over the table data and populate the expected
attributes based on the lookup table provided to the table loader.
"""
from wstlr import (
        evaluate_bool, 
        _data_dictionary_type_map, 
        system_base, 
        dd_system_url
)
import re
import sys
import pdb

class DdVariable:
    def __init__(self, 
                    study_name, 
                    table_name, 
                    url_base=system_base, 
                    **kwargs):
        self.url_base=url_base

        self.study_name = study_name
        self.table_name = table_name
        self.varname = kwargs["variable_name"]
        self.description = kwargs.get("description", "")
        self.data_type = self.parse_data_type(kwargs.get("data_type", "string"))
        self.enumerations = self.parse_enums(kwargs.get("enumerations"))

        self.consent_group = kwargs.get("consent_group")
        self.study_component = study_name
        if self.consent_group is not None:
            self.study_component = f"{study_name}-{self.consent_group}"

        self.url = dd_system_url(self.url_base, 
                                    "CodeSystem", 
                                    self.study_component, 
                                    table_name, 
                                    self.varname)

        # These are not currently used by Whistler, at least not in this way 
        # as of the time of writing this
        self.key_component = evaluate_bool(kwargs.get("key_component", False))
        self.required = evaluate_bool(kwargs.get("required", False))
        self.notes = kwargs.get("notes", "")

    def add_to_varname_lookup(self, lkup):
        desc = self.desc 

        if desc != self.varname:
            lkup[desc] = self.varname

        for code, description in self.enumerations.items():
            if code != description:
                vardesc = f"{self.varname}:{description}"
                lkup[vardesc] = code

    @property
    def desc(self):
        if self.description is not None and len(self.description.strip()) != 0:
            return self.description
        return self.varname

    def parse_data_type(self, data_type):
        for dt in _data_dictionary_type_map.keys():
            if data_type.lower() in _data_dictionary_type_map[dt]:
                return _data_dictionary_type_map[dt][0]
        # if it doesn't match, why not just report the problem and exit
        sys.stderr.write(f"""Unrecognized variable type, {data_type}. """
            """Please see about adding this type to the categories in """
            """Whistler.\n""")
        sys.exit(1)

    def parse_enums(self, values):
        transformed_values = {}

        if values is None:
            return transformed_values

        if type(values) is str:
            splitter = ";"

            if splitter not in values:
                if '\n' in values:
                    splitter='\n'
            split_values = values.split(splitter)
        else:
            split_values = values
        for entry in split_values:
            if "=" in entry:
                code,desc = entry.split("=")[0:2]
                if code not in transformed_values:
                    transformed_values[code.strip()] = desc.strip()
            else:
                if len(split_values) > 1 and entry.strip() != "":
                    transformed_values[entry.strip()] = entry.strip()

        return transformed_values

    def obj_as_dd_variable(self):
        """Build out dd entries for the variable's whistle input"""
        obj = {
            "varname": self.varname,
            "desc": self.desc,
            "type": self.data_type,
            "values": self.values_for_json()
        }
        if len(obj['values']) > 0:
            obj['values-url'] = self.url
            obj['values-details'] = {
                "table_name": self.table_name,
                "varname": self.varname
            }
        return obj 

    def obj_as_dd(self):
        """Build out dd entries for the variable's whistle input"""
        obj = {
            "varname": self.varname,
            "desc": self.desc,
            "type": self.data_type,
            "values": self.values_for_json()
        }
        if len(obj['values']) > 0:
            obj['values-url'] = self.url
            obj['values-details'] = {
                "table_name": self.table_name,
                "varname": self.varname
            }
        return obj 

    def obj_as_cs(self):
        """Prepare for dumping to the whistle input json file for """
        """code-system"""
        obj = {
            "varname": self.varname,
            "url": self.url,
            "study": self.study_name,
            "table_name": self.table_name,
            "values": self.values_for_json()
        }
        if self.varname is not None:
            obj['varname'] = self.varname

        if self.consent_group is not None:
            obj['consent_group'] = self.consent_group
        return obj
    
    def values_for_json(self):
        """Build out the values suitable for adding to json object"""
        values = []

        for code in self.enumerations:
            desc = self.enumerations[code]

            if desc is None or desc == 'None' or desc.strip() == "":
                desc = code
            values.append({
                "code": code,
                "description": desc
            })

        return values