# Classes to support building row level data entries

from wstlr.extractor import fix_fieldname
import sys

_string_types = set(['string', ''])
_integer_types = set(['int'])
_float_types = set(['number'])



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
