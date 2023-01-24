"""
Parse JSON Version of DD
"""
from wstlr import die_if, fix_fieldname 
from wstlr.dd.loader import DdLoader
from wstlr.dd.table import DdTable

from pathlib import Path
import csv

import json

import pdb

class JsonParser(DdLoader):
    def __init__(self, filename, tables_path="tables", columns_path="columns"):
        # We need to know the name of the variable containing the list
        # of variables 
        self.tables_path = tables_path
        self.variables_path = columns_path
        # We'll set the study_name and description using data from the JSON 
        # file itself. 
        super().__init__(filename=filename, study_name=filename, description="")

    def open(self, filename=None):
        die_if(filename is None, "No filename provided for JSON file")

        file = self.open_file(filename)
        data = json.load(file)

        self.name = data['name']
        self.description = data['description']

        for table in data[self.tables_path]:
            table_name = table['table']
            self.study.add_table(name=table_name)

            for variable in table[self.variables_path]:
                variable = {self.colnames[varname] if varname in self.colnames else varname:value for varname, value in variable.items()}
                self.study.add_variable(table_name, **variable)

    def convert_to_csv(self, destdir):
        dest = Path(destdir) / fix_fieldname(self.name)
        dest.mkdir(parents=True, exist_ok=True)

        for table_name, table in self.study.tables.items():
            filename = dest / (table_name + ".csv")

            with filename.open('wt') as f:
                writer = csv.writer(f, delimiter=',', quotechar='"')
                writer.writerow([
                    "variable_name",
                    "description",
                    "data_type",
                    "enumerations",
                    "notes"
                ])

                for varname, variable in table.variables.items():
                    enumerations = variable.enumerations
                    if len(enumerations) == 0:
                        enumerations = ""
                    writer.writerow([
                        varname,
                        variable.description,
                        variable.data_type,
                        enumerations,
                        variable.notes
                    ])

def convert_json_to_csv():
    import sys
    from argparse import ArgumentParser, FileType

    parser = ArgumentParser(
        description="Convert data dictionary from a single JSON file to CSV files"
    )
    parser.add_argument(
        "-s",
        "--source",
        type=str,
        help="The filename or URL where the JSON object can be found"
    )
    parser.add_argument(
        "-t",
        "--table-array-variable",
        type=str,
        default="tables",
        help="Variable name where the array of tables is found. "
    )
    parser.add_argument(
        "-c",
        "--column-array-variable",
        type=str,
        default="columns",
        help="Variable name where the array of columns is found within the table object."
    )
    parser.add_argument(
        "-o",
        "--out-directory",
        type=str,
        default="data/dd/csv",
        help="Directory where CSV files will be written"
    )
    args = parser.parse_args(sys.argv[1:])

    jsonp = JsonParser(filename=args.source,
                        tables_path=args.table_array_variable,
                        columns_path=args.column_array_variable)
    
    jsonp.convert_to_csv(args.out_directory)