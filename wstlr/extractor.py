#!/usr/bin/env python

import csv
import sys
import json
from yaml import safe_load
from argparse import ArgumentParser, FileType
from pathlib import Path
import re
from collections import defaultdict
from copy import deepcopy

import pdb

system_base = "https://nih-ncpi.github.io/ncpi-fhir-ig"

default_colnames = {
    "varname": "varname",
    "desc": "vardesc",
    "type": "type",
    "units": "units",
    "min": "min",
    "max": "max",
    "values": "values"
}

def get_data(colname, row, namelist):
    #pdb.set_trace()
    if colname in namelist:
        return row[namelist[colname]].strip()
    return None

def store_data(colname, row, dest, namelist):
    value = get_data(colname, row, namelist)

    if value is not None:
        dest[colname] = value

xcleaner = re.compile(";\s+")
def clean_values(valuestring):
    """I'm seeing some spaces in the value lists, but they aren't consistant, so we'll strip them out"""
    if valuestring is None:
        return ""
    return re.sub(xcleaner, ';', valuestring.strip())

class DataDictionaryVariableCS:
    def __init__(self, study, table_name, varname, values):
        self.varname = varname

        if varname is None:
            self.url = f"{system_base}/CS/data-dictionary/{study}/{table_name}"
        else:
            self.url = f"{system_base}/CS/data-dictionary/{study}/{table_name}/{varname}"
        self.study = study
        self.table_name = table_name
        self.values = self.extract_values(values)

    def extract_values(self, values):
        transformed_values = {}
        splitter = ";"

        if splitter not in values:
            if '\n' in values:
                splitter='\n'
        #pdb.set_trace()
        split_values = values.split(splitter)
        for entry in split_values:
            if "=" in entry:
                code,desc = entry.split("=")[0:2]
                if code not in transformed_values:
                    transformed_values[code.strip()] = desc.strip()
            else:
                if len(split_values) > 1 and entry.strip() != "":
                    transformed_values[entry.strip()] = entry.strip()

        return transformed_values

    def as_obj(self):
        """Prepare for dumping to the whistle input json file"""
        obj = {
            "url": self.url,
            "study": self.study,
            "table_name": self.table_name,
            "values": self.values_for_json()
        }
        if self.varname is not None:
            obj['varname'] = self.varname

        return obj
    
    def values_for_json(self):
        """Build out the values suitable for adding to json object"""
        values = []

        for code in self.values:
            values.append({
                "code": code,
                "description": self.values[code]
            })
        return values

def ObjectifyDD(study_id, table_name, dd_file, dd_codesystems, colnames=None): 
    """DDs are treated differently. Rather than an array of objects, it's one object with select columns as properties
    
    Values are aggregated into key/value objects where the value is the key and the meaning is the value
    """

    print(f"The table's name is: {table_name}")

    global default_colnames
    if colnames is None:    
        colnames = default_colnames

    dd_content = {
        "table_name": table_name,
        "variables": []
    }
    reader = csv.DictReader(dd_file, delimiter=',', quotechar='"')
    reader.fieldnames = [x.lower() for x in reader.fieldnames]
    #pdb.set_trace()
    table_cs_values = []
    for line in reader:
        try:
            varname = get_data('varname', line, colnames)
            desc = get_data('desc', line, colnames)
        except BaseException as e:
            print(f"An issue was found extracting data from file, {dd_file.name}")
            print(f"with header: {colnames} + {line.keys()}")
            print(e)
            sys.exit(1)

        table_cs_values.append(f"{varname}={desc}")
        variable = {
            'varname': varname,
        }
        #pdb.set_trace()
        for colname in colnames.keys():
            if colname not in ["varname", 'values']:
                store_data(colname, line, variable, colnames)

        if 'values' in colnames:
            variable['values'] = []
            vname = colnames['values']
            
            values = clean_values(line[vname])
            if values not in dd_codesystems:
                dd_codesystems[values] = DataDictionaryVariableCS(study_id, table_name, varname, values)
            variable['values'] = dd_codesystems[values].values_for_json()
            variable['values-details'] = {
                'table-name': dd_codesystems[values].table_name,
                'varname': dd_codesystems[values].varname
            }
            variable['values-url'] = dd_codesystems[values].url

        dd_content['variables'].append(variable)


    if table_name in dd_codesystems:
        pdb.set_trace()
        print(f"We have already processed the table, {table_name}")
    assert(table_name not in dd_codesystems)
    #pdb.set_trace()
    dd_codesystems[table_name] = DataDictionaryVariableCS(study_id, table_name, None, ";".join(table_cs_values))

    return dd_content

def TestAggregatable(aggregators, varname):
    for rgx in aggregators.keys():
        if rgx.search(varname):
            return aggregators[rgx]
    return None

def AggregateColumns(aggregators, colnames):
    # Standard columns will go straight as root properties of the current object
    standard_columns = set()

    # Aggregated columns will end up nested as properties of the varname "property"
    aggregated_columns = defaultdict(set)

    for fieldname in colnames:
        aggregated = False

        for rgx in aggregators.keys():
            if rgx.search(fieldname):
                aggregated_columns[aggregators[rgx]].add(fieldname)
                aggregated = True
                break
        if not aggregated:
            standard_columns.add(fieldname)    

    return (standard_columns, aggregated_columns)

def ObjectifyCSV(csv_file, aggregators={}, agg_splitter=None, code_details={}, varname_lkup={}):
    """Transform columnar data into objects where each row becomes individual objects and columns become properties for those objects
    
    :param csv_file: File to be transformed. This should be an open file.
    :type csv_file: Readable file object
    :param aggregators: Dictionary of compiled regexs to aggregate multiple columns as properties inside a single variable
    :type aggregators: Dictionary
    :param code_details: code => text chunk to pass into the objects for the "text" portion of codings
    :type code_details: dictionary 
    """
    data_chunk = []

    #pdb.set_trace()
    reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
    reader.fieldnames = [x.lower().replace(" ", "_").replace(")", "").replace("(", "").replace("/", "_") for x in reader.fieldnames]

    # Standard columns will go straight as root properties of the current object
    # Aggregated columns will end up nested as properties of the varname "property"
    standard_columns, aggregated_columns = AggregateColumns(aggregators, reader.fieldnames)

    for line in reader:
        row = line
        if len(aggregated_columns) > 0:
            row = {}
            for col in standard_columns:
                varname = col
                row[varname] = line[col]            

            for newcol in aggregated_columns.keys():
                qname = newcol
                if newcol in varname_lkup:
                    newcol = varname_lkup[newcol]
                row[newcol] = []

                for var in aggregated_columns[qname]:
                    code_var = var
                    if agg_splitter is not None and agg_splitter in code_var:
                        code_var = agg_splitter.join(code_var.split(agg_splitter)[1:])
                    
                    varname = code_var
                    varidentifier = f"{newcol}:{varname}"
                    #pdb.set_trace()
                    if varidentifier in varname_lkup:
                        varname = varname_lkup[varidentifier]
                    coding = {"code": varname, "value": line[var]}
                    if var in code_details:
                        coding['text'] = code_details[var]
                    row[newcol].append(coding)

        for col in standard_columns:
            if line[col] in code_details:
                row[f"{col}_display"] = code_details[row[col]]
            
        data_chunk.append(row)

    return data_chunk

def BuildAggregators(cfg_agg):
    aggregators = {}
    for varname in cfg_agg.keys():
        regex = cfg_agg[varname]
        regex = re.compile(regex, re.I)
        
        aggregators[regex] = varname    
    return aggregators

def DataCsvToObject(config):
    dataset = {
        "study": {
            "id": config['study_id'],
            "accession": config['study_accession'],
            "title": config['study_title'],
            "desc": config['study_desc'],
            "identifier-prefix": config['identifier_prefix'],
            "url": config['url'],
            "data-dictionary": []
        },
        "code-systems": []
    }

    dd_codesystems = {}
    for category in config['dataset'].keys():
        data_chunk = []
        aggregators = {}
        agg_splitter = None
        if 'aggregators' in config['dataset'][category]:
            agg_splitter = config['dataset'][category].get('aggregator-splitter')
            aggregators = BuildAggregators(config['dataset'][category]['aggregators'])
            #print(aggregators)
        code_details = {}
        if 'code_harmonization' in config['dataset'][category]:
            with open(config['dataset'][category]['code_harmonization'], 'rt') as f:
                reader = csv.DictReader(f, delimiter=',', quotechar='"')

                for row in reader:
                    code_details[row['local code']] = row['display']

        # For some datasets, there may be an set of artificial question "names" or values 
        # which won't appear in the actual data. We'll need to scan this for the "description" 
        # to identify those artificial questions and assign those to the final output instead
        # of the long, descriptive name
        dd_based_varnames = {}
        if 'data_dictionary' in config['dataset'][category]:
            with open(config['dataset'][category]['data_dictionary']['filename'], encoding='utf-8-sig') as f:
                dd = ObjectifyDD(config['study_id'], category, f, dd_codesystems, config['dataset'][category]['data_dictionary'].get('colnames'))
                dataset['study']['data-dictionary'].append(dd)
                dd_based_varnames = build_varname_lookup(dd)

        with open(config['dataset'][category]['filename'], encoding='utf-8-sig', errors='ignore') as f:
            data_chunk = ObjectifyCSV(f, aggregators, agg_splitter, code_details, dd_based_varnames)

        dataset[category] = data_chunk

    for key in dd_codesystems:
        dataset['code-systems'].append(dd_codesystems[key].as_obj())
    return dataset

def build_varname_lookup(dd):
    lookup = {}
    for var in dd['variables']:
        if var['desc'].strip() != "" and var['desc'] != var['varname']:
            lookup[var['desc']] = var['varname']
        for value in var['values']:
            #pdb.set_trace()
            code = value['code']
            desc = value['description']

            if code != desc:
                vardesc = f"{var['varname']}:{desc}"
                if vardesc not in lookup:
                    lookup[vardesc] = code
                else:
                    if lookup[vardesc] != code:
                        print(f"Houston, we have a problem with variable: {vardesc}")
                        print(f"{lookup[vardesc]} != {code}")
                    assert(lookup[vardesc] == code)
    
    return lookup

def exec(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = ArgumentParser()
    parser.add_argument("config",
                        type=FileType('rt', encoding='utf-8-sig'),
                        nargs=1,
                        help="YAML file containing configuration details")
    parser.add_argument("-o",
                        "--output-root",
                        default='output/whistle-input')

    args = parser.parse_args()
    config = safe_load(args.config[0])

    # Work out the destination for the Whistle input
    output_directory = Path(args.output)
    output_directory.mkdir(parents=True, exist_ok=True)
    output_filename = output_directory / f"{config['output_filename']}.json"

    dataset = DataCsvToObject(config, args.output_root, count=args.count)
    with output_filename.open(mode='wt') as f:
        f.write(json.dumps(dataset, indent=2))
        
        
