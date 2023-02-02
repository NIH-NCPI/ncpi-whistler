"""
Parse CSV version of DD
"""

from wstlr import die_if, system_base
from wstlr.dd.loader import DdLoader

from pathlib import Path
import csv
import pdb

class CsvParser(DdLoader):
    def __init__(self, filename, 
                    name, 
                    description="", 
                    colnames={},
                    url_base=system_base):
        super().__init__(filename, 
                            name, 
                            description, 
                            colnames=colnames, 
                            url_base=url_base)

    def open(self, filename, name=None, colnames={}):
        die_if(filename is None, "No filename provided for CSV file")

        self.set_colnames(colnames)

        if name is None:
            name = Path(filename).stem

        self.study.add_table(name=name)
        file = self.open_file(filename)

        reader = csv.DictReader(file, delimiter=",", quotechar='"')
        
        fieldnames = []
        for colname in reader.fieldnames:
            fieldnames.append(self.colnames.get(colname, colname))
            
        reader.fieldnames = fieldnames
        
        # Sanity check the key columns we require
        self.check_for_required_colnames(fieldnames)

        for line in reader:
            self.study.add_variable(name, **line)
