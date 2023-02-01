"""
Base class for data-dictionary loaders

"""

import re
import requests
from copy import deepcopy 

from wstlr.dd.study import DdStudy
from wstlr import die_if, system_base

from tempfile import TemporaryFile
import sys

import pdb

class DdLoader:
    required_columns =  [
        "variable_name",
        "data_type",
        "enumerations"
    ]
    def __init__(self, filename, 
                        study_name, 
                        description="", 
                        colnames={},
                        url_base=system_base):
        self.filename = filename
        self.name = study_name
        self.description = description
        self.url_base = url_base

        if self.name is None:
            self.name = self.filename
        
        self.study = DdStudy(name=self.name, 
                        description=self.description,
                        url_base=self.url_base)
        
        # when we open a CSV file, we'll walk over the header and replace 
        # the following keys with their values, which is what we will use 
        # during runtime
        self.base_colnames = {
            "column": "variable_name",
            "varname": "variable_name",
            "desc": "description",
            "type": "data_type",
            "values": "enumerations"
        }

        self.open(filename=self.filename, colnames=colnames)

    def open_file(self, filename):
        """Can open files from internet or local-returns the file object"""

        # We should support files that start with http: 
        httpx = re.compile("^http[s]*:")
        if httpx.search(filename):
            response = requests.get(filename)
            file = TemporaryFile()
            file.write(response.content)
            file.seek(0)
        else:
            file = open(filename, 'rt')

        return file


        
    def set_colnames(self, alternate_names):
        self.colnames = deepcopy(self.base_colnames)

        for alt_name, target_name in alternate_names.items():
            self.colnames[alt_name] = target_name

    def check_for_required_colnames(self, fieldnames):
        colnames = set(fieldnames)

        for required_col in DdLoader.required_columns:
            die_if(required_col not in fieldnames, f"Required column, "
                f"{required_col}, missing from file, {self.filename}. If this "
                f"should be mapped to another column, please provide a valid "
                f"mapping.")

    def open(self, filename=None):
        # This should be overridden by the real classes doing the hard work. 
        assert(False)