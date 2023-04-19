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
                    table_name=None,
                    colnames={},
                    url_base=system_base):
        super().__init__(filename, 
                            name, 
                            description, 
                            colnames=colnames, 
                            url_base=url_base)

        self.open(filename=self.filename, name=table_name, colnames=colnames)

    def open(self, filename, name=None, colnames={}):
        die_if(filename is None, "No filename provided for CSV file")

        self.set_colnames(colnames)

        print(f"New CSV: {filename} : {name}")
        if name is None:
            name = Path(filename).stem

        self.study.add_table(name=name)
        file = self.open_file(filename)

        # We have some excess columns...because, why not. 
        # restkey should prevent those from kill python's dereference
        reader = csv.DictReader(file, delimiter=",", quotechar='"', restkey='junk')
        
        fieldnames = []
        for colname in reader.fieldnames:
            fieldnames.append(self.colnames.get(colname, colname))
            
        reader.fieldnames = fieldnames
        
        # Sanity check the key columns we require
        self.check_for_required_colnames(fieldnames)

        for line in reader:
            self.study.add_variable(name, **line)
