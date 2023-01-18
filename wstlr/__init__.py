__version__="0.0.1"

from pathlib import Path
from yaml import safe_load
import sys
from ncpi_fhir_client import fhir_auth
import pdb
from collections import OrderedDict
from enum import Enum
import re

system_base = "https://nih-ncpi.github.io/ncpi-fhir-ig"

class DDVariableType(Enum):
    StringType = 1
    IntegerType = 2
    FloatType = 3
    CategoricalType = 4
    DateType = 5

# The formal representation we'll pass to whistle will be the first entry in 
# each list Because categoricals actually do require values, the underlying 
# functionality should default to capturing strings if there are inadequate 
# enumerated values in the data dictionary provided
_data_dictionary_type_map = OrderedDict()
_data_dictionary_type_map[DDVariableType.StringType] = ['string', '', 'str']
_data_dictionary_type_map[DDVariableType.IntegerType] = ['int', 'integer']
_data_dictionary_type_map[DDVariableType.FloatType] = ['number', 'decimal']
_data_dictionary_type_map[DDVariableType.CategoricalType] = ['string', 'integer, encoded value']
_data_dictionary_type_map[DDVariableType.DateType] = ['date']

class TableType(Enum):
    Default = 1
    Embedded = 2
    Grouped = 3

class InvalidType(Exception):
    def __init__(self, bad_type):
        self.type_name = bad_type

    def message(self):
        return f"""Unrecognized variable type, {dd_type}. Please see """
        """about adding this type to the categories in Whistler.\n"""

def StandardizeDdType(dd_type):
    for dt in _data_dictionary_type_map.keys():
        the_types =  _data_dictionary_type_map[dt]
        if dd_type.lower() in _data_dictionary_type_map[dt]:
            return _data_dictionary_type_map[dt][0]

    raise InvalidType(dd_type)

def determine_table_type(table_def):
    """Checks for specific keys to determine which TableType applies"""
    if "embed" in table_def:
        return TableType.Embedded
    if "group_by" in table_def:
        return TableType.Grouped
    return TableType.Default

def example_config(writer, auth_type=None):
    """Returns a block of text containing one or all possible auth modules example configurations"""

    modules = fhir_auth.get_modules()
    print(
        f"""# Example Hosts Configuration.
# 
# This is a basic yaml file (yaml.org) where each root level tag represents a 
# system "name" and it's children's keys represent key/values to assign to a 
# host configuration which includes the authentication details.
#
# All host entries should have the following key/values:
# host_desc             - This is just a short description which can be used
#                         for log names or whatnot
# target_service_url    - This is the URL associated with the actual API 
# auth_type             - This is the module name for the authentication used
#                         by the specified host
#
# Please note that there can be multiple hosts that use the same authentication
# mechanism. Users must ensure that each host has a unique "key" """,
        file=writer,
    )
    for key in modules.keys():
        if auth_type is None or auth_type == key:
            other_entries = {
                "host_desc": f"Example {key}",
                "target_service_url": "https://example.fhir.server/R4/fhir",
            }

            modules[key].example_config(writer, other_entries)


def get_host_config():
    host_config_filename = Path("fhir_hosts")

    if not host_config_filename.is_file() or host_config_filename.stat().st_size == 0:
        example_config(sys.stdout)
        die_if(True, f""" 
A valid host configuration file, fhir_hosts, must exist in cwd and was not 
found. Example configuration has been written to stout providing examples 
for each of the auth types currently supported.\n"""
        )

    return safe_load(host_config_filename.open("rt"))

def die_if(do_die, msg, errnum=1):
    if do_die:
        sys.stderr.write(msg + "\n")
        sys.exit(errnum)

xcleaner = re.compile(";\s+")
def clean_values(valuestring):
    """I'm seeing some spaces in the value lists, but they aren't consistant, so we'll strip them out"""
    if valuestring is None:
        return ""
    return re.sub(xcleaner, ';', valuestring.strip())

def fix_fieldname(fieldname):
    return fieldname.lower().replace(" ", "_").replace(")", "").replace("(", "").replace("/", "_")


def dd_system_url(url_base, term_type, study_component, table_name, varname ):
    if varname is None:
        return f"{url_base}/{term_type}/data-dictionary/{study_component}/{fix_fieldname(table_name)}"
    else:
        return f"{url_base}/{term_type}/data-dictionary/{study_component}/{fix_fieldname(table_name)}/{fix_fieldname(varname)}"
