"""
Parse CSV version of DD
"""

from wstlr import die_if 
import csv

class CsvParser(DdLoader):
    def __init__(self, name, description=""):
        super().__init__(name, description)

    def open(self, filename=None, name=None):
        die_if(filename is None, "No filename provided for CSV file")

        if name is None:
            name = filename.split("/")[-1]

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
            self.study.add_variable(name, line)
