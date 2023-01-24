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
from wstlr import evaluate_bool, _data_dictionary_type_map
import re
import sys
import pdb

class DdVariable:
    def __init__(self, **kwargs):
        self.varname = kwargs["variable_name"]
        self.description = kwargs.get("description", "")
        self.data_type = self.parse_data_type(kwargs.get("data_type", "string"))
        self.enumerations = self.parse_enums(kwargs.get("enumerations"))

        # These are not currently used by Whistler, at least not in this way 
        # as of the time of writing this
        self.key_component = evaluate_bool(kwargs.get("key_component", False))
        self.required = evaluate_bool(kwargs.get("required", False))
        self.notes = kwargs.get("notes", "")

    def parse_data_type(self, data_type):
        for dt in _data_dictionary_type_map.keys():
            if data_type.lower() in _data_dictionary_type_map[dt]:
                return _data_dictionary_type_map[dt][0]
        # if it doesn't match, why not just report the problem and exit
        sys.stderr.write(f"""Unrecognized variable type, {data_type}. Please see """
            """about adding this type to the categories in Whistler.\n""")
        sys.exit(1)

    def parse_enums(self, values):
        #pdb.set_trace()
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