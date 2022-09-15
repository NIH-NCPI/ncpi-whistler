# Questionnaire and Questionnaire Responses

"""
This class will generate the whistle code to process row-level data as QuestionnaireResponses. 

"""
from wstlr import TableType, determine_table_type

from wstlr.extractor import ObjectifyDD, fix_fieldname
from wstlr.sourcedata.sourcedata_base import srcd_questionnaire_def_base
from wstlr.sourcedata import _string_types, _integer_types, _float_types, identifier_columns, MissingKeyColumns

from pathlib import Path
from yaml import safe_load
import sys
from argparse import ArgumentParser, FileType
from string import Template

import pdb

def WriteWhistleFile(whistle_src_dir, row_level_fns, process_fns, entry_point):
    filename = f"{whistle_src_dir}/source_data_questionnaires.wstl"
    with open(filename, 'wt') as f:
        f.write(srcd_questionnaire_def_base + "\n")

        f.write("// Write each row to an observation, specialized for each of the tables\n")
        for table in row_level_fns:
            f.write(table + "\n")
        
        f.write("// Process each table separately, passing each individual row separately")
        for table in process_fns:
            f.write(table + "\n")
    
        f.write(f"// The entry point for all QR Raw Data production\n{entry_point}")

    return filename


def BuildQuestionnaire(outvar, ddtable, filename, idcols):
    table_name = ddtable['table_name']

    table_desc = ddtable.get("desc")
    if table_desc is None:
        table_desc = filename

    if table_desc is None:
        table_desc = table_name

    qitems = []

    for variable in ddtable['variables']:
        varname = variable['varname']
        vartype = variable['type'].lower()
        if 'values' in variable and len(variable['values']) > 0:
            #pdb.set_trace()
            qitems.append(f"    component[]: ReportQuestionnaireItemCategorical(study, table_name, \"{varname}\", \"{variable['desc']}\", \"choice\", \"{variable['values-url']}\");")
        elif vartype in _string_types:
            qitems.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"string\");")
        elif vartype in _integer_types:
            qitems.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"integer\");")
        elif vartype in _float_types:
            qitems.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"decimal\");")
        else:
            print(f"What do we do with this one? {varname} is {vartype}")
            pdb.set_trace()
            qitems.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"string\");")


    row_composition = ""
    row_init = ""
    if len(items) > 0:
        items = "\n".join(items)

        row_composition = Template("""
def AddSourceDataQuestionnaire-${table_name}(study, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Questionnaire", $$StrCat(study.id, ".${table_name}"));
    identifier[0].use: "official";

    url: BuildQuestionnaireURL(study, table_name);
    name: table_name;
    title: \"${table_desc}\";
    status: "active";
    subjectType[]: "Patient";
    code[]: , "https://loinc.org/"("74468-0", "https://loinc.org/", "Questionnaire form definition Document");
    code[]: HarmonizeMapped(table_name, "DataSet");

    resourceType: "Questionnaire";
$items
}""").substitute(table_name=table_name, table_desc=table_desc, items=items)

    row_init = Template("""

def ProcessQuestionnaire-${table_name}(study, rows) {
    out $outvar: AddSourceDataQuestionnaire-${table_name}(study);
    out $outvar: AddSourceDataQuestionnaireResponse-${table_name}(study, rows[]);
}        
        """).substitute(table_name=table_name, outvar=outvar)
        
    return (row_composition, row_init)

def BuildQuestionnaireStandard(outvar, table_name, table_desc, subject_reference, subject_type, key_columns, qitems, items):

    row_composition = Template("""
// Build the Questionnaire itself
def AddSourceDataQuestionnaire-${table_name}(study) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Questionnaire", $$StrCat(study.id, ".", table_name));
    identifier[0].use: "official";

    url: BuildQuestionnaireURL(study, table_name);
    name: table_name;
    title: \"${table_desc}\";
    status: "active";
    ${subjtype}
    code[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code[]: HarmonizeMapped(table_name, "DataSet");

    resourceType: "Questionnaire";
$qitems
}

def AddSourceDataQuestionnaireResponse-${table_name}(study, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier: Key_Identifier(study, "QuestionnaireResponse", $$StrCat(study.id, ".", table_name, ".", "source-data", ".", ${idcols}));
    identifier.use: "official";
    status: "completed ";
    resourceType: "QuestionnaireResponse";
    questionnaire: BuildQuestionnaireURL(study, table_name);
    ${subject_reference}
$items
}""").substitute(table_name=table_name, qitems=qitems, items=items, subject_reference=subject_reference, table_desc=table_desc, idcols=key_columns, subjtype=subject_type)

    row_init = Template("""

def ProcessQuestionnaire-${table_name}(study, rows) {
    out $outvar: AddSourceDataQuestionnaire-${table_name}(study);
    out $outvar: AddSourceDataQuestionnaireResponse-${table_name}(study, rows[]);
}        
        """).substitute(table_name=table_name, outvar=outvar)

    entry_fn = f"   $this: ProcessQuestionnaire-{table_name}(resource.study, resource.{table_name}[*]);"
    return (row_composition, row_init, entry_fn)

def BuildQuestionnaireEmbedded(outvar, table_name, parent_table, table_desc, subject_reference, subject_type, colnames, key_columns, qitems, items):

    row_composition = Template("""
// Build the Questionnaire itself
def AddSourceDataQuestionnaire-${table_name}(study) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Questionnaire", $$StrCat(study.id, ".", table_name));
    identifier[0].use: "official";

    url: BuildQuestionnaireURL(study, table_name);
    name: table_name;
    title: \"${table_desc}\";
    status: "active";
    ${subjtype}
    code[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code[]: HarmonizeMapped(table_name, "DataSet");

    resourceType: "Questionnaire";
$qitems
}

def AddSourceDataQuestionnaireResponse-${table_name}(study, id, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier: Key_Identifier(study, "QuestionnaireResponse", $$StrCat(study.id, ".", table_name, ".", "source-data", ".", id, ".", ${idcols}));
    identifier.use: "official";
    status: "completed ";
    resourceType: "QuestionnaireResponse";
    questionnaire: BuildQuestionnaireURL(study, table_name);
    ${subject_reference}
$items
}""").substitute(table_name=table_name, qitems=qitems, items=items, subject_reference=subject_reference, table_desc=table_desc, idcols=key_columns, subjtype=subject_type)

    row_init = Template("""

def ProcessQuestionnaire-${table_name}(study, row) {
    var id: $$StrCat(${colnames});
    out $outvar: AddSourceDataQuestionnaireResponse-${table_name}(study, id, row.${table_name}[]);
}   

def ProcessQuestionnairePrep-${table_name}(study, rows) {
    out $outvar: AddSourceDataQuestionnaire-${table_name}(study);
    out $outvar: ProcessQuestionnaire-${table_name}(study, rows[]);
}        
        """).substitute(table_name=table_name, colnames=colnames, outvar=outvar)

    entry_fn = f"   $this: ProcessQuestionnairePrep-{table_name}(resource.study, resource.{parent_table}[*]);"
    return (row_composition, row_init, entry_fn)

def BuildQuestionnaireGrouped(outvar, table_name, table_desc, subject_reference, subject_type, group_columns, key_columns, qitems, items):
    row_composition = Template("""
// Build the Questionnaire itself
def AddSourceDataQuestionnaire-${table_name}(study) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Questionnaire", $$StrCat(study.id, ".", table_name));
    identifier[0].use: "official";

    url: BuildQuestionnaireURL(study, table_name);
    name: table_name;
    title: \"${table_desc}\";
    status: "active";
    ${subjtype}
    code[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code[]: HarmonizeMapped(table_name, "DataSet");

    resourceType: "Questionnaire";
$qitems
}

def AddSourceDataQuestionnaireResponse-${table_name}(study, id, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier: Key_Identifier(study, "QuestionnaireResponse", $$StrCat(study.id, ".", table_name, ".", "source-data", ".", id, ".", ${idcols}));
    identifier.use: "official";
    status: "completed ";
    questionnaire: BuildQuestionnaireURL(study, table_name);
    resourceType: "QuestionnaireResponse";
    ${subject_reference}
$items
}""").substitute(table_name=table_name, qitems=qitems, items=items, subject_reference=subject_reference, table_desc=table_desc, idcols=key_columns, subjtype=subject_type)

    row_init = Template("""

def ProcessQuestionnaireRow-${table_name}(study, row) {
    var id: $$StrCat(${groupcols});
    $$this: AddSourceDataQuestionnaireResponse-${table_name}(study, id, row.content[]);
}

def ProcessQuestionnaire-${table_name}(study, rows) {
    out $outvar: AddSourceDataQuestionnaire-${table_name}(study);
    out $outvar: ProcessQuestionnaireRow-${table_name}(study, rows[]);
}        
        """).substitute(table_name=table_name, groupcols=group_columns, outvar=outvar)

    entry_fn = f"   $this: ProcessQuestionnaire-{table_name}(resource.study, resource.{table_name}[*]);"
    return (row_composition, row_init, entry_fn)



def BuildSrcDataProcessor(outvar, ddtable, ddconfig, filename, id_colname):
    table_name = ddtable['table_name']
    table_type = determine_table_type(ddconfig)
    subject_id_col = ddconfig.get('subject_id')

    table_desc = ddtable.get("desc")
    if table_desc is None:
        table_desc = filename

    if table_desc is None:
        table_desc = table_name

    subjectid = "SUBJECTID_REPLACE_ME"
    if subject_id_col is None:
        subject_id_col = id_colname
    if subject_id_col:
        subjectid=subject_id_col

    items = []
    qitems = []

    for variable in ddtable['variables']:
        varname = variable['varname']
        vartype = variable['type'].lower()
        if 'values' in variable and len(variable['values']) > 0:
            qitems.append(f"    item[]: BuildQuestionnaireItemCategorical(study, table_name, \"{varname}\", \"{variable['desc']}\", \"choice\", \"{variable['values-url']}\");")
            items.append(f"    item[]: ReportQuestionnaireItemCategorical(study, \"{varname}\", \"{variable['desc']}\" , row_data.{fix_fieldname(varname)});")
        elif vartype in _string_types:
            qitems.append(f"    item[]: BuildQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"string\");")
            items.append(f"    item[]: ReportQuestionnaireItemBasic(study, \"{varname}\", \"{variable['desc']}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _integer_types:
            qitems.append(f"    item[]: BuildQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"integer\");")
            items.append(f"    item[]: ReportQuestionnaireItemInteger(study, \"{varname}\", \"{variable['desc']}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _float_types:
            qitems.append(f"    item[]: BuildQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"decimal\");")
            items.append(f"    item[]: ReportQuestionnaireItemQuantity(study, \"{varname}\", \"{variable['desc']}\", row_data.{fix_fieldname(varname)});")
        else:
            print(f"What do we do with this one? {varname} is {vartype}")
            qitems.append(f"    item[]: BuildQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"string\");")
            items.append(f"    item[]: ReportQuestionnaireItemBasic(study, \"{varname}\", \"{variable['desc']}\", row_data.{fix_fieldname(varname)});")

    row_composition = ""
    row_init = ""
    if len(items) > 0:
        items = "\n".join(items)
        qitems = "\n".join(qitems)

        if subjectid is not None and subjectid != "NONE":
            subject_reference = f"""subject: Reference_Key_Identifier(study, "Patient", row_data.{subjectid});"""
            subject_type = "subjectType[]: \"Patient\";"
        else:
            subject_reference =""
            subject_type = ""
        
        if table_type == TableType.Default:
            key_columns = identifier_columns(ddconfig.get('key_columns'), subjectid)
            row_composition, row_init, entry_fn = BuildQuestionnaireStandard(outvar, 
                                                                    table_name,
                                                                    table_desc, 
                                                                    subject_reference,
                                                                    subject_type, 
                                                                    key_columns,
                                                                    qitems,
                                                                    items
                                                                    )
        elif table_type == TableType.Grouped:
            key_columns = identifier_columns(ddconfig.get('key_columns'), subjectid)
            group_columns = identifier_columns(ddconfig.get("group_by"), subjectid, "row")
            row_composition, row_init, entry_fn = BuildQuestionnaireGrouped(outvar, 
                                                                    table_name,
                                                                    table_desc, 
                                                                    subject_reference,
                                                                    subject_type, 
                                                                    group_columns,
                                                                    key_columns,
                                                                    qitems,
                                                                    items)
        elif table_type == TableType.Embedded:
            key_columns = identifier_columns(ddconfig.get('key_columns'), subjectid)
            group_columns = identifier_columns(ddconfig['embed'].get("colname"), subjectid, "row")
            parent_name = ddconfig['embed']['dataset']

            row_composition, row_init, entry_fn = BuildQuestionnaireEmbedded(outvar, 
                                                                    table_name, 
                                                                    parent_name,
                                                                    table_desc,
                                                                    subject_reference,
                                                                    subject_type, 
                                                                    group_columns,
                                                                    key_columns, 
                                                                    qitems,
                                                                    items)
        
    return (row_composition, row_init, entry_fn)


def exec():
    parser = ArgumentParser(
        description="Generate whistle code that will produce source data as QuestionnaireResponse."
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

                subjidcol = config['dataset'][category].get('subject_id')
                if subjidcol is None:
                    subjidcol = fix_fieldname(config['id_colname'])
                key_columns = config['dataset'][category].get('key_columns')

                with open(config['dataset'][category]['data_dictionary']['filename'], 'rt', encoding='utf-8-sig') as f:
                    delimiter = ","
                    if 'delimiter' in config['dataset'][category]['data_dictionary']:
                        delimiter = config['dataset'][category]['data_dictionary']['delimiter']

                    dd, cs_values = ObjectifyDD(config['study_id'], consent_group, category, f, dd_codesystems, config['dataset'][category]['data_dictionary'].get('colnames'), delimiter=delimiter)
  
                    try:
                        (row_composition, row_init, entry_fn) = BuildSrcDataProcessor("source_data", 
                                                                            dd, 
                                                                            config['dataset'][category],
                                                                            filename=config['dataset'][category]['data_dictionary']['filename'],
                                                                            id_colname=fix_fieldname(config['id_colname'])
                                                                            )
                    except MissingKeyColumns as e:
                        sys.stderr.write(e.message(category) + "\n")
                        sys.exit(1)
                    data_functions.append(row_composition)
                    process_functions.append(row_init)
                    entry_fns.append(entry_fn)

        if len(data_functions) > 0:
            entry_point = """def BuildRawDataQR(resource) {\n""" + "\n".join(entry_fns) + "\n}\n"""

            filename = WriteWhistleFile(whistle_src_dir, data_functions, process_functions, entry_point)
            print(f"File created: {filename}")
        else:
            print(f"No file created this time")
