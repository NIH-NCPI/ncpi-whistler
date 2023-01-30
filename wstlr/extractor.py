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
from wstlr.conceptmap import ObjectifyHarmony
from wstlr.embedable import EmbedableTable
from wstlr import dd_system_url, StandardizeDdType, clean_values, fix_fieldname

from wstlr import system_base, InvalidType

from wstlr.config import Configuration

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
    if colname in namelist:
        varname = namelist[colname]
        if varname in row:
            if row[varname] is not None:
                return row[varname].strip()
    return None

def store_data(colname, row, dest, namelist):
    value = get_data(colname, row, namelist)

    if value is not None:
        dest[colname] = value



class GroupBy:
    def __init__(self, config=None):
        self.group_by = []
        self.content = {}

        if config is not None:
            self.group_by=[fix_fieldname(x) for x in config.split(",")]
        else:
            self.content = []
    
    def parse(self, row):

        current = self.content

        if len(self.group_by) > 0:
            key = ":".join([row[x] for x in self.group_by])

            if key not in self.content:
                self.content[key] = {
                    "content": []
                }
                for var in self.group_by:
                    self.content[key][var] = row[var]

            current = self.content[key]['content']
    
        cur_row = {}
        for var in row:
            if var not in self.group_by:
                cur_row[var] = row[var]
        current.append(cur_row)

    def collect(self):
        """Return the objectified contents of the group_by variable(s)"""

        results = []

        if len(self.group_by) > 0:
            for key in self.content.keys():
                results.append(self.content[key])
        else:
            results = self.content
        
        return results

        for col in self.group_by:
            if row[col] not in current:
                current[row[col]] = {
                    col: row[col],
                    "content": {}
                }
            current = self.content[row[col]]['content']
        
        for col in row.keys():
            if col not in self.group_by:
                current[col] = row[col]

class DataDictionaryVariableCS:
    def __init__(self, study, consent_group, table_name, varname, values, url_base=system_base):
        self.varname = varname

        self.study_component = study
        if consent_group is not None:
            self.study_component = f"{study}-{consent_group}"

        self.url = dd_system_url(url_base, "CodeSystem", self.study_component, table_name, varname)

        self.study = study
        self.consent_group = consent_group
        self.table_name = table_name
        self.values = self.extract_values(values)

    def extract_values(self, values):
        transformed_values = {}
        splitter = ";"

        if splitter not in values:
            if '\n' in values:
                splitter='\n'
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

        if self.consent_group is not None:
            obj['consent_group'] = self.consent_group
        return obj
    
    def values_for_json(self):
        """Build out the values suitable for adding to json object"""
        values = []

        for code in self.values:
            desc = self.values[code]

            if desc is None or desc == 'None':
                desc = code
            values.append({
                "code": code,
                "description": desc
            })
            if values[-1]['description'].strip() == "":
                values[-1]['description'] = code
        return values

def ObjectifyDD(study_id, 
                    consent_group, 
                    table_name, 
                    dd_file, 
                    dd_codesystems, 
                    colnames=None, 
                    delimiter=",",
                    subject_id=None, 
                    url_base=system_base): 
    """DDs are treated differently. Rather than an array of objects, it's one object with select columns as properties
    
    Values are aggregated into key/value objects where the value is the key and the meaning is the value
    """
    global default_colnames
    if colnames is None:    
        colnames = {}
    
    for key in default_colnames:
        if key not in colnames:
            colnames[key] = default_colnames[key]

    study_component = study_id
    if consent_group is not None:
        study_component = f"{study_component}-{consent_group}"

    dd_content = {
        "table_name": table_name,
        "url": dd_system_url(url_base, "CodeSystem", study_component, table_name, varname=None),
        "variables": []
    }

    # Allow the configuration to specify the fieldname associated with the subject's ID. This will be necessary for generating
    # whistle code for the row level representation. 
    if subject_id is not None:
        dd_content['subject_id'] = subject_id

    reader = csv.DictReader(dd_file, delimiter=delimiter, quotechar='"')
    reader.fieldnames = [x.lower() for x in reader.fieldnames]

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

        # When we have descriptions that are highly descriptive, we have to 
        # be careful that they don't end up being interpreted to be enumerating
        # variables. 
        table_cs_values.append(f"{varname}={desc.replace('=', ' is equal to ').replace(';', '.')}")

        variable = {
            'varname': varname,
        }

        for colname in colnames.keys():
            if colname not in ["varname", 'values']:
                store_data(colname, line, variable, colnames)
        if 'type' in variable:
            try:
                variable['type'] = StandardizeDdType(variable['type'])
            except InvalidType as e:
                print(f"Unrecognized variable type, {e.type_name}, found in "
                      f"dictionary for table, {table_name}. ")
                sys.exit(1)
        if variable['desc'].strip() == "":
            variable['desc'] = variable['varname']

        if 'values' in colnames:
            variable['values'] = []
            vname = colnames['values']
            
            values = clean_values(line[vname])
            if values not in dd_codesystems:
                dd_codesystems[values] = DataDictionaryVariableCS(study_id, consent_group, table_name, varname, values, url_base=url_base)
            variable['values'] = dd_codesystems[values].values_for_json()
            if len(values) > 0:
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

    dd_codesystems[table_name] = DataDictionaryVariableCS(study_id, consent_group, table_name, None, ";".join(table_cs_values), url_base=url_base)

    return dd_content, table_cs_values

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


def ObjectifyCSV(csv_file, aggregators={}, grouper=None, agg_splitter=None, code_details={}, varname_lkup={}, delimiter=","):
    """Transform columnar data into objects where each row becomes individual objects and columns become properties for those objects
    
    :param csv_file: File to be transformed. This should be an open file.
    :type csv_file: Readable file object
    :param aggregators: Dictionary of compiled regexs to aggregate multiple columns as properties inside a single variable
    :type aggregators: Dictionary
    :param code_details: code => text chunk to pass into the objects for the "text" portion of codings
    :type code_details: dictionary 
    """
    data_chunk = []

    reader = csv.DictReader(csv_file, delimiter=delimiter, quotechar='"')
    reader.fieldnames = [fix_fieldname(x) for x in reader.fieldnames]

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

                    if varidentifier in varname_lkup:
                        varname = varname_lkup[varidentifier]
                    coding = {"code": varname, "value": line[var]}
                    if var in code_details:
                        coding['text'] = code_details[var]
                    row[newcol].append(coding)

        for col in standard_columns:
            if line[col] in code_details:
                row[f"{col}_display"] = code_details[row[col]]
        grouper.parse(row)
        #data_chunk.append(row)

    return grouper.collect()

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
            "id": config.study_id,
            "accession": config.study_accession,
            "title": config.study_title,
            "desc": config.study_desc,
            "identifier-prefix": config.identifier_prefix,
            "url": config.url,
            "publisher": config.publisher,
            "data-dictionary": [
                {
                    "study": config.study_id,
                    "values": []
                }
            ]
        },
        "code-systems": [],
        "harmony": [],
    }

    if config.remote_data_access is not None:
        dataset['study']['remote_access'] = {
            "type": config.remote_data_access['access_type'],
            "url": config.remote_data_access['access_url']
        }

    if config.study_sponsor is not None:
        dataset['study']['sponsor'] = config.study_sponsor

    if config.consent_group is not None:
        dataset['study']['consent_group'] = config.consent_group
        dataset['study']['consent_code'] = config.consent_group['code']
    
    if dataset['study']['publisher'] is None:
        dataset['study']['publisher'] = "NCPI FHIR Working Group"

    consent_group = dataset['study'].get('consent_code')
    study_component = dataset['study']['id']
    if consent_group is not None:
        study_component = f"{study_component}-{consent_group}"

    dd_tablevar_cs = DataDictionaryVariableCS(config.study_id, 
                                    consent_group, 
                                    "DataSet", 
                                    None, 
                                    "", 
                                    url_base=config.identifier_prefix)

    dataset['study']['data-dictionary'][0] = dd_tablevar_cs.as_obj()

    active_tables = config.active_tables
    if active_tables is None:
        active_tables['ALL'] = True

    dd_codesystems = {}
    harmony_files = set()

    embedded = defaultdict(list)

    for category,table in config.dataset.items():
        embedable = table.get('embed')

        filenames = table['filename'].split(",")

        dd_tablevar_cs.values[category] = ",".join([fn.split("/")[-1] for fn in filenames])

        # This is a cheap way to indicate that our dataset doesn't have any real 
        # data. But, we don't want anything to believe that there is a file out
        # there named 'none'.
        if filenames[0].lower() == 'none':
            dd_tablevar_cs.values[category] = None

        if embedable is not None:
            embd = EmbedableTable(category, embedable['dataset'], embedable['colname'])
            for filename in table['filename'].split(","):
                embd.load_data(filename)
            embedded[embd.target].append(embd)

    for category, table in config.dataset.items():
        data_chunk = []
        aggregators = {}
        agg_splitter = None
        if 'aggregators' in table:
            agg_splitter = table.get('aggregator-splitter')
            aggregators = BuildAggregators(table['aggregators'])
        
        code_details = {}
        if 'code_harmonization' in table:
            with open(table['code_harmonization'], 'rt') as f:
                reader = csv.DictReader(f, delimiter=',', quotechar='"')

                for row in reader:
                    if 'display' not in row:
                        print(row)
                    code_details[row['local code']] = row['display']
        dataset['study']['data-dictionary'][0]['values'].append({
            "varname": category,
            "desc": ",".join([x.split("/")[-1] for x in table['filename'].split(",")]),
            "type": "DD-Table",
            "url": dd_system_url(config.identifier_prefix, 
                                "CodeSystem", 
                                study_component, 
                                category, 
                                varname=None),
            "values": []
        })


        # For some datasets, there may be an set of artificial question "names" or values 
        # which won't appear in the actual data. We'll need to scan this for the "description" 
        # to identify those artificial questions and assign those to the final output instead
        # of the long, descriptive name
        dd_based_varnames = {}
        if 'data_dictionary' in table:
            # print(config['dataset'][category]['data_dictionary']['filename'])
            with open(table['data_dictionary']['filename'], 'rt', encoding='utf-8-sig') as f:
                delimiter = ","
                if 'delimiter' in table['data_dictionary']:
                    delimiter = table['data_dictionary']['delimiter']

                dd, cs_values = ObjectifyDD(config.study_id, 
                                                consent_group, 
                                                category, 
                                                f, 
                                                dd_codesystems, 
                                                table['data_dictionary'].get('colnames'), 
                                                delimiter=delimiter, 
                                                subject_id=table.get("subject_id"),
                                                url_base=config.identifier_prefix)
                
                # fill out the 'values' list for each of the vars
                for cs_entry in cs_values:
                    varname, desc = cs_entry.split("=")
                    if desc.strip() == "":
                        desc = varname
                    dataset['study']['data-dictionary'][0]['values'][-1]['values'].append({
                        "code": varname,
                        "description": desc
                    })
                if active_tables.get('ALL') == True or active_tables.get("data-dictionary"):
                    dataset['study']['data-dictionary'].append(dd)
                dd_based_varnames = build_varname_lookup(dd)

        if active_tables.get('ALL') == True or active_tables.get("harmony"):
            if 'code_harmonization' in table:
                harmony_file = table['code_harmonization']
                if harmony_file not in harmony_files:
                    harmony_files.add(harmony_file)
                    dataset['harmony'].append(ObjectifyHarmony(harmony_file, 
                                                            curies=config.curies, 
                                                            study_component=study_component))

        if active_tables.get('ALL') == True or active_tables.get(category):
            if 'embed' not in table:
                grouper = GroupBy(config=table.get('group_by'))
                file_list = [x.strip() for x in table['filename'].split(",")]

                for filename in file_list:
                    if filename.lower() != 'none':
                        with open(filename, encoding='utf-8-sig', errors='ignore') as f:
                            delimiter = ","
                            if 'delimiter' in table:
                                delimiter = table['delimiter']

                            data_chunk = ObjectifyCSV(f, 
                                                    aggregators, 
                                                    grouper, 
                                                    agg_splitter, 
                                                    code_details, 
                                                    dd_based_varnames, 
                                                    delimiter=delimiter)

                            if category in embedded:
                                for emb in embedded[category]:
                                    if emb.join_col not in data_chunk[0]:
                                        print(f"Unable to find column, '{emb.join_col}', options include: {data_chunk[0].keys()} Unable to embed this table.")
                                    for row in data_chunk:
                                        if emb.join_col not in row:
                                            print(f"Unable to find join column: {emb.join_col}. \nAvailable columns: {','.join(sorted(row.keys()))}")
                                        row[emb.table_name] = emb.get_rows(row[emb.join_col])

                        dataset[category] = data_chunk
        else:
            print(f"Skipping in-active table, {category}")

    # Add our main dataset CS to the list
    dataset['code-systems'].append(dd_tablevar_cs.as_obj())
    for key in dd_codesystems:
        dataset['code-systems'].append(dd_codesystems[key].as_obj())
    return dataset

def build_varname_lookup(dd):
    lookup = {}
    for var in dd['variables']:

        var['desc'] = var['desc']
        if var['desc'].strip() != "" and var['desc'] != var['varname']:
            lookup[var['desc']] = var['varname']

        for value in var['values']:

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

    args = parser.parse_args(args=args)

    config = Configuration(args.config[0])

    # Work out the destination for the Whistle input
    output_directory = Path(args.output)
    output_directory.mkdir(parents=True, exist_ok=True)
    output_filename = output_directory / f"{config.output_filename}.json"

    dataset = DataCsvToObject(config, args.output_root, count=args.count)
    with output_filename.open(mode='wt') as f:
        f.write(json.dumps(dataset, indent=2))
        
        
