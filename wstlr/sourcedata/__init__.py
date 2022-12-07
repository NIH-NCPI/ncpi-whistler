# Classes to support building row level data entries

from wstlr import TableType, determine_table_type
from wstlr.extractor import fix_fieldname
import sys

_string_types = set(['string', ''])
_integer_types = set(['int', 'integer'])
_float_types = set(['number', 'decimal'])



class MissingKeyColumns(Exception):
    def __init__(self):
        super().__init__(self.message())
    
    def message(self, table=None):
        if table:
            return f"{table} has subject_id=NONE and as such, must have a valid key_columns entry specifying the column names which establish a unique row"
        return "Any data-table with subject_id=NONE must have a valid key_columns entry specifying the column names which establish a unique row"

# We'll construct identifier columns out of key_columns or whatever the 
# subject_id column is defined to be when key_columns is None
# 
# key_columns can be a comma separated list (including just a single value with
# no comma) or an array. These should be the actual column names for the 
# columns being referred to
def identifier_columns(key_columns, subject_id, varpath='row_data'):
    if (key_columns is None and subject_id is None):
        sys.stderr.write("You must have a subject_id column or key columns in the configuration. Please see docs that do not currently exist!\n")
        sys.exit(1)

    if key_columns is None:
        if subject_id == "NONE":
            raise MissingKeyColumns()
            
        key_columns = [subject_id]
    elif type(key_columns) is str:
        if "," in key_columns:
            key_columns = [x.strip() for x in key_columns.split(",")]
        else:
            key_columns = [key_columns]
    return ", \".\", ".join(f"{varpath}.{fix_fieldname(x)}" for x in key_columns)


class DdVariable:
    def __init__(self, variable):
        self.varname = variable['varname']
        self.vartype = variable['type']
        self.desc = variable['desc']
        self.values_url = variable.get("values-url")
        self.fieldname = fix_fieldname(self.varname)

        if 'values' in variable and len(variable['values']) > 1:
            self.vartype = "enumeration"
        elif self.vartype in _string_types:
            self.vartype = "string"
        elif self.vartype in _integer_types:
            self.vartype = "integer"
        elif self.vartype in _float_types:
            self.vartype = "float"
class DdTable:
    def __init__(self, ddtable, ddconfig, id_colname):
        self.table_name = ddtable['table_name']

        self.desc = ddtable.get("desc")
        if self.desc is None:
            self.desc = ddconfig['filename']

        self.id_col = ddconfig.get('subject_id')
        if self.id_col is None:
            self.id_col = id_colname
        
        if self.id_col is None:
            self.id_col = "SUBJECTID_REPLACE_ME!!!"
        self.variables = []

        self.colnames = identifier_columns(id_colname, id_colname, 'row')
        if 'key_columns' in ddconfig:
            self.colnames = identifier_columns(ddconfig['key_columns'], id_colname, 'row') #[fix_fieldname(x) for x in ddconfig['key_columns'].split(",")]
        self.table_type = determine_table_type(ddconfig)
        self.parent_table = self.table_name
        if self.table_type == TableType.Embedded:
            self.parent_table = ddconfig['embed']['dataset']
            #self.colnames = [fix_fieldname(x) for x in ddconfig['embed']['colname'].split(",")]
            #self.colnames = identifier_columns(ddconfig['embed']['colname'], id_colname, 'row') 
        elif self.table_type == TableType.Grouped:
            self.colnames = identifier_columns(ddconfig['group_by'], id_colname, 'row') #[fix_fieldname(x) for x in ddconfig['group_by'].split(",")]

        for variable in ddtable['variables']:
            self.variables.append(DdVariable(variable))