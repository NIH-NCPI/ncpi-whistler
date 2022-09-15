# Observations with Components

"""
This class will generate the whistle code to process row-level data as 
observations with components where table is treated as a single function to be 
called on each row of said table. 

The class should generate the functions required to populate each component
dependent on the column's type. 

"""
from wstlr import TableType, determine_table_type
from wstlr.extractor import ObjectifyDD, fix_fieldname
from wstlr.sourcedata.sourcedata_base import srcd_observation_def_base
from wstlr.sourcedata import _string_types, _integer_types, _float_types, identifier_columns, MissingKeyColumns
from pathlib import Path
from yaml import safe_load
import sys
from argparse import ArgumentParser, FileType
from string import Template

import pdb

def WriteWhistleFile(whistle_src_dir, source_data_fns, process_fns, entry_point):
    filename = f"{whistle_src_dir}/source_data_observations.wstl"
    with open(filename, 'wt') as f:
        f.write(srcd_observation_def_base + "\n")

        f.write("// Write each row to an observation, specialized for each of the tables\n")
        for table in source_data_fns:
            f.write(table + "\n")
        
        f.write("// Process each table separately, passing each individual row separately")
        for table in process_fns:
            f.write(table + "\n")

        f.write(f"// The entry point for all Obs Raw Data production\n{entry_point}")

    return filename

def BuildSubjectReference(subjectid, key_columns=None):
    if key_columns is not None and subjectid not in key_columns:
        return ""
    if subjectid is not None and subjectid != "NONE":
        return f"""subject: Reference_Key_Identifier(study, "Patient", row_data.{subjectid});"""
    else:
        return ""


def BuildEmbeddedProcessors(table_name, parent_table, components, subjectid, colnames, key_columns, outvar):
    row_composition = Template("""
def AddSourceDataObservation-${table_name}(study, id, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Observation", $$StrCat(study.id, ".", table_name, ".", id , ".", ${idcols}));
    identifier[0].use: "official";
    status: "final";
    resourceType: "Observation";
    code.coding[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code.coding[]: HarmonizeMapped(table_name, "DataSet");
    code.text: $$StrCat("Source data for data table, ", table_name);
    ${subject_reference}
$components
}""").substitute(table_name=table_name, 
                    components=components, 
                    idcols=key_columns,
                    subject_reference=BuildSubjectReference(subjectid, key_columns))

    row_init = Template("""
def ProcessSourceDataLevel-${table_name}(study, row) {
    var id: $$StrCat(${colnames});
    out $outvar: AddSourceDataObservation-${table_name}(study, id, row.${table_name}[]);
}        
        """).substitute(table_name=table_name, 
                        colnames=colnames, 
                        outvar=outvar)

    entry_fn = f"   $this: ProcessSourceDataLevel-{table_name}(resource.study, resource.{parent_table}[]);"
    return row_composition, row_init, entry_fn

def BuildGroupedProcessors(table_name, components, subjectid, group_cols, key_columns, outvar):
    row_composition = Template("""
def AddSourceDataObservation-${table_name}(study, id, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Observation", $$StrCat(study.id, ".", table_name, ".", id , ".", ${idcols}));
    identifier[0].use: "official";
    status: "final";
    resourceType: "Observation";
    code.coding[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code.coding[]: HarmonizeMapped(table_name, "DataSet");
    code.text: $$StrCat("Source data for data table, ", table_name);
    ${subject_reference}
$components
}""").substitute(table_name=table_name, 
                    components=components, 
                    idcols=key_columns,
                    subject_reference=BuildSubjectReference(subjectid, key_columns))

    row_init = Template("""
def ProcessSourceDataLevel-${table_name}(study, row) {
    var id: $$StrCat(${groupcols});
    out $outvar: AddSourceDataObservation-${table_name}(study, id, row.content[]);
}        
        """).substitute(table_name=table_name, 
                        groupcols=group_cols, 
                        outvar=outvar)

    entry_fn = f"   $this: ProcessSourceDataLevel-{table_name}(resource.study, resource.{table_name}[]);"
    return row_composition, row_init, entry_fn

def BuildStandardProcessors(table_name, components, subjectid, key_columns, outvar):
    row_composition = Template("""
def AddSourceDataObservation-${table_name}(study, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Observation", $$StrCat(study.id, ".", table_name, ".", "source-data", ".", ${idcols}));
    identifier[0].use: "official";
    status: "final";
    resourceType: "Observation";
    code.coding[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code.coding[]: HarmonizeMapped(table_name, "DataSet");
    code.text: $$StrCat("Source data for data table, ", table_name);
    ${subject_reference}
$components
}""").substitute(table_name=table_name, 
                    components=components, 
                    idcols=key_columns, 
                    subject_reference=BuildSubjectReference(subjectid, key_columns))

    row_init = Template("""
def ProcessSourceDataLevel-${table_name}(study, row) {
    out $outvar: AddSourceDataObservation-${table_name}(study, row);
}        
        """).substitute(table_name=table_name, outvar=outvar)

    entry_fn = f"   $this: ProcessSourceDataLevel-{table_name}(resource.study, resource.{table_name}[]);"
    return row_composition, row_init, entry_fn

def BuildSrcLProcessor(outvar, ddtable, ddconfig, id_colname):
    table_name = ddtable['table_name']
    subjectid = "SUBJECTID_REPLACE_ME"
    table_type = determine_table_type(ddconfig)
    subject_id_col = ddconfig.get('subject_id')
    if subject_id_col is None:
        subject_id_col = id_colname
    if subject_id_col:
        subjectid=subject_id_col

    components = []

    vars_captured = 0
    for variable in ddtable['variables']:
        varname = variable['varname']
        vartype = variable['type'].lower()
        if 'values' in variable and len(variable['values']) > 0:
            components.append(f"    component[]: BuildObsComponentCategorical(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _string_types:
            components.append(f"    component[]: BuildObsComponentString(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _integer_types:
            components.append(f"    component[]: BuildObsComponentInteger(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _float_types:
            components.append(f"    component[]: BuildObsComponentQuantity(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")
        else:
            print(f"What do we do with this one? {varname} is {vartype}")
            pdb.set_trace()
            components.append(f"    component[]: BuildObsComponentString(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")

    row_composition = ""
    row_init = ""
    if len(components) > 0:
        components = "\n".join(components)
        strcat = "$StrCat"

        if table_type == TableType.Default:    
            key_columns = identifier_columns(ddconfig.get('key_columns'), subjectid)

            row_composition, row_init, entry_fn = BuildStandardProcessors(table_name, components, subjectid, key_columns, outvar)
        elif table_type == TableType.Grouped:
            key_columns = identifier_columns(ddconfig.get('key_columns'), subjectid)
            group_columns = identifier_columns(ddconfig.get("group_by"), subjectid, "row")
            row_composition, row_init, entry_fn = BuildGroupedProcessors(table_name, components, subjectid, group_columns, key_columns, outvar)
        elif table_type == TableType.Embedded:
            key_columns = identifier_columns(ddconfig.get('key_columns'), subjectid)
            group_columns = identifier_columns(ddconfig['embed'].get("colname"), subjectid, "row")
            parent_name = ddconfig['embed']['dataset']

            row_composition, row_init, entry_fn = BuildEmbeddedProcessors(table_name, parent_name, components, subjectid, group_columns, key_columns, outvar)

        
    return (row_composition, row_init, entry_fn)


def exec():
    parser = ArgumentParser(
        description="Generate whistle code that will produce source data as observations with components."
    )
    parser.add_argument(
        "config",
        nargs='+',
        type=FileType('rt'),
        help="Dataset YAML file with details used to build the necessary code.",
    )
    args = parser.parse_args(sys.argv[1:])

    for config_file in args.config:
        config = safe_load(config_file)

        if 'projector_lib' in config:
            whistle_src_dir = Path(config['projector_lib'])
        else:
            print(f"There is no whistle source directory, projector_lib,  specified in the configuration file. Writing whistle code to 'projector'. ")
            whistle_src_dir = Path("projector")
        whistle_src_dir.mkdir(exist_ok=True, parents=True)

        consent_group = None
        if 'consent_group' in config:
            consent_group = config['consent_group']['code']
        
        files_created = []
        dd_codesystems = {}

        # We'll keep the data portions separately from the process part so that
        # the process functions will be there at the very bottom of the file for
        # clarity
        data_functions = []
        process_functions = []
        entry_fns = []
        for category in config['dataset'].keys():
            if 'data_dictionary' in config['dataset'][category]:
                table_type = determine_table_type(config['dataset'][category])

                with open(config['dataset'][category]['data_dictionary']['filename'], 'rt', encoding='utf-8-sig') as f:
                    delimiter = ","
                    if 'delimiter' in config['dataset'][category]['data_dictionary']:
                        delimiter = config['dataset'][category]['data_dictionary']['delimiter']

                    dd, cs_values = ObjectifyDD(config['study_id'], consent_group, category, f, dd_codesystems, config['dataset'][category]['data_dictionary'].get('colnames'), delimiter=delimiter)
  
                    try:
                        (row_composition, row_init, entry_fn) = BuildSrcLProcessor("source_data", 
                                                                    dd, 
                                                                    config['dataset'][category],
                                                                    fix_fieldname(config['id_colname']))
                    except MissingKeyColumns as e:
                        sys.stderr.write(e.message(category) + "\n")
                        sys.exit(1)
                    data_functions.append(row_composition)
                    process_functions.append(row_init)
                    entry_fns.append(entry_fn)

        if len(data_functions) > 0:
            entry_point = """def BuildRawDataObs(resource) {\n""" + "\n".join(entry_fns) + "\n}\n"""
            filename = WriteWhistleFile(whistle_src_dir, data_functions, process_functions, entry_point)
            print(f"File created: {filename}")
        else:
            print(f"No file created this time")
