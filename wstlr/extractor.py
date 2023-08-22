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

import pdb

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

def TestAggregatable(aggregators, varname):
    for rgx in aggregators.keys():
        if rgx.search(varname):
            return aggregators[rgx]
    return None

def AggregateColumns(aggregators, colnames):
    # Standard columns will go straight as root properties of the current
    # object
    standard_columns = set()

    # Aggregated columns will end up nested as properties of the varname 
    # "property"
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


def ObjectifyCSV(csv_file, 
                aggregators={}, 
                grouper=None, 
                agg_splitter=None, 
                code_details={}, 
                varname_lkup={}, 
                delimiter=","):
    """Transform columnar data into objects where each row becomes """
    """individual objects and columns become properties for those objects
    
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

    return grouper.collect()

def BuildAggregators(cfg_agg):
    aggregators = {}
    for varname in cfg_agg.keys():
        regex = cfg_agg[varname]
        regex = re.compile(regex, re.I)
        
        aggregators[regex] = varname    
    return aggregators

def DataCsvToObject(config):
    #pdb.set_trace()
    dataset = {
        "config": {
            "missing": ["NA", "", "Not Provided"]
        },
        "study": {
            "id": config.study_id,
            "accession": config.study_accession,
            "title": config.study_title,
            "desc": config.study_desc,
            "identifier-prefix": config.identifier_prefix,
            "dd-prefix": config.dd_prefix,
            "url": config.url,
            "publisher": config.publisher,
            "data-dictionary": [
                {
                    "study": config.study_id,
                    "values": []
                }
            ],
            "annotations": config.annotations
        },
        "code-systems": [],
        "harmony": [],
    }

    if config.config is not None:
        if "missing" in config.config:
            dataset['config'] = config.config['missing'].split(",")

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

    dataset['study']['data-dictionary'][0] = config.study_dd.obj_as_dd()
    dataset['code-systems'].append(config.study_dd.obj_as_cs())

    active_tables = config.active_tables
    if active_tables is None:
        active_tables['ALL'] = True

    harmony_files = set()

    embedded = defaultdict(list)

    for category,table in config.dataset.items():
        embedable = table.get('embed')
        #filenames = table['filename'].split(",")

        if embedable is not None:
            embd = EmbedableTable(category, embedable['dataset'], embedable['colname'])
            for filename in table['filename'].split(","):
                embd.load_data(filename)
            embedded[embd.target].append(embd)


    for category,table in config.dataset.items():
        agg_splitter = table.get('aggregator-splitter')
        aggregators = {}
        if 'aggregators' in table:
            aggregators = BuildAggregators(table.get('aggregators'))

        code_details = {}
        if 'code_harmonization' in table:
            with open(table['code_harmonization'], 'rt') as f:
                reader = csv.DictReader(f, delimiter=',', quotechar='"')

                for row in reader:
                    if 'display' not in row:
                        print(row)
                    code_details[row['local code']] = row['display']
        #pdb.set_trace()
        if 'data_dictionary' in table:
            if table['data_dictionary']['filename'].lower() != 'none':
                with open(table['data_dictionary']['filename'], 'rt', encoding='utf-8-sig') as f:
                    delimiter = ","
                    if 'delimiter' in table['data_dictionary']:
                        delimiter = table['data_dictionary']['delimiter']

                # Unlike data, we don't want data-dictionary components 
                # disappearing due to inactive tables. That control is 
                # intended for data loading
                table_dd = config.study_dd.table_as_dd(category)
                if table_dd:
                    dataset['study']['data-dictionary'].append(table_dd)

        if active_tables.get('ALL') == True or active_tables.get("harmony"):
            if 'code_harmonization' in table:
                harmony_file = table['code_harmonization']
                if harmony_file not in harmony_files:
                    harmony_files.add(harmony_file)
                    dataset['harmony'].append(ObjectifyHarmony(harmony_file, 
                                                            curies=config.curies, 
                                                            study_component=study_component))

        # For some datasets, there may be an set of artificial question "names" or values 
        # which won't appear in the actual data. We'll need to scan this for the "description" 
        # to identify those artificial questions and assign those to the final output instead
        # of the long, descriptive name
        dd_based_varnames = config.study_dd.varname_lookup(category)

        # Add our main dataset CS to the list
        table_cs = config.study_dd.table_as_cs(category)
        if table_cs:
            dataset['code-systems'].append(table_cs)
        else:
            print(f"{category} didn't produce a table cs")
        
        newcs = config.study_dd.variables_as_cs(category)
        if newcs:
            dataset['code-systems'] += newcs
        
        if active_tables.get('ALL') == True or active_tables.get(category):
            print(f"Processing active table, {category}")
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

    return dataset

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
        
        
