# Questionnaire and Questionnaire Responses

"""
This class will generate the whistle code to process row-level data as QuestionnaireResponses. 

"""

from wstlr.extractor import ObjectifyDD, fix_fieldname
from wstlr.rowlevel.row_level_base import rl_questionnaire_def_base
from wstlr.rowlevel import _string_types, _integer_types, _float_types
from pathlib import Path
from yaml import safe_load
import sys
from argparse import ArgumentParser, FileType
from string import Template

import pdb

def WriteWhistleFile(whistle_src_dir, row_level_fns, process_fns):
    filename = f"{whistle_src_dir}/row_level_questionnaires.wstl"
    with open(filename, 'wt') as f:
        f.write(rl_questionnaire_def_base + "\n")

        f.write("// Write each row to an observation, specialized for each of the tables\n")
        for table in row_level_fns:
            f.write(table + "\n")
        
        f.write("// Process each table separately, passing each individual row separately")
        for table in process_fns:
            f.write(table + "\n")

    return filename

def BuildQuestionnaire(outvar, ddtable, filename):
    table_name = ddtable['table_name']

    table_desc = ddtable.get("desc")
    if table_desc is None:
        table_desc = filename

    if table_desc is None:
        table_desc = table_name

    items = []

    for variable in ddtable['variables']:
        varname = variable['varname']
        vartype = variable['type'].lower()
        if 'values' in variable and len(variable['values']) > 0:
            #pdb.set_trace()
            items.append(f"    component[]: ReportQuestionnaireItemCategorical(study, table_name, \"{varname}\", \"{variable['desc']}\", \"choice\", \"{variable['values-url']}\");")
        elif vartype in _string_types:
            items.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"string\");")
        elif vartype in _integer_types:
            items.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"integer\");")
        elif vartype in _float_types:
            items.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"decimal\");")
        else:
            print(f"What do we do with this one? {varname} is {vartype}")
            pdb.set_trace()
            items.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", \"{variable['desc']}\", \"string\");")


    row_composition = ""
    row_init = ""
    if len(items) > 0:
        items = "\n".join(items)

        row_composition = Template("""
def AddRowLevelQuestionnaire${table_name}(study, row_data) {
    var table_name: "${table_name}";
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Questionnaire", $$StrCat(study.id, ".${table_name}"));
    identifier[0].use: "official";

    url: BuildQuestionnaireURL(study, table_name);
    name: table_name;
    title: \"${table_desc}\";
    status: "active";
    subjectType[]: "Patient";
    code[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code[]: HarmonizeMapped(table_name, "DataSet");

    resourceType: "Questionnaire";
$items
}""").substitute(table_name=table_name, table_desc=table_desc, items=items)

    row_init = Template("""

def ProcessQuestionnaire${table_name}(study, row) {
    out $outvar: AddRowLevelQuestionnaire${table_name}(study, row);
}        
        """).substitute(table_name=table_name, outvar=outvar)
        
    return (row_composition, row_init)

def BuildRLProcessor(outvar, ddtable, subject_id_col=None):
    table_name = ddtable['table_name']
    subjectid = "SUBJECTID_REPLACE_ME"

    if subject_id_col:
        subjectid=subject_id_col

    items = []

    for variable in ddtable['variables']:
        varname = variable['varname']
        vartype = variable['type'].lower()
        if 'values' in variable and len(variable['values']) > 0:
            items.append(f"    component[]: BuildQuestionnaireItemCategorical(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _string_types:
            items.append(f"    component[]: BuildQuestionnaireItemBasic(study, table_name,  \"{varname}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _integer_types:
            items.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")
        elif vartype in _float_types:
            items.append(f"    component[]: ReportQuestionnaireItemBasic(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")
        else:
            print(f"What do we do with this one? {varname} is {vartype}")
            pdb.set_trace()
            items.append(f"    component[]: BuildObsComponentString(study, table_name, \"{varname}\", row_data.{fix_fieldname(varname)});")

    row_composition = ""
    row_init = ""
    if len(items) > 0:
        items = "\n".join(items)
        row_composition = Template("""
def AddRowLevelQuestionnaireResponse${table_name}(study, row_data) {
    var table_name: "${table_name}";
    var subjid: row_data.${subjectidcol};
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "QuestionnaireResponse", $$StrCat(study.id, ".", table_name, ".", subjid));
    identifier[0].use: "official";
    status: "final";
    resourceType: "QuestionnaireResponse";
    code.coding[]: BuildCoding("74468-0", "Questionnaire form definition Document", "https://loinc.org/");
    code.coding[]: HarmonizeMapped(table_name, "DataSet");
    code.text: $$StrCat("Row level data for data table, ", table_name);

    // We do have a subject...
    subject: Reference_Key_Identifier(study, "Patient", subjid);
$items
}""").substitute(table_name=table_name, items=items, subjectidcol=subjectid)

    row_init = Template("""

def ProcessQuestionnaireResponse${table_name}(study, row) {
    out $outvar: AddRowLevelQuestionnaireResponse${table_name}(study, row);
}        
        """).substitute(table_name=table_name, outvar=outvar)
        
    return (row_composition, row_init)


def exec():
    parser = ArgumentParser(
        description="Generate whistle code that will produce row-level data as QuestionnaireResponse."
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
        for category in config['dataset'].keys():
            if 'data_dictionary' in config['dataset'][category]:
                with open(config['dataset'][category]['data_dictionary']['filename'], 'rt', encoding='utf-8-sig') as f:
                    delimiter = ","
                    if 'delimiter' in config['dataset'][category]['data_dictionary']:
                        delimiter = config['dataset'][category]['data_dictionary']['delimiter']

                    dd, cs_values = ObjectifyDD(config['study_id'], consent_group, category, f, dd_codesystems, config['dataset'][category]['data_dictionary'].get('colnames'), delimiter=delimiter)
  
                    #pdb.set_trace()
                    (row_composition, row_init) = BuildRLProcessor("row_level", dd, subject_id_col=config['dataset'][category].get('subject_id'))
                    data_functions.append(row_composition)
                    process_functions.append(row_init)

                    (row_composition, row_init) = BuildQuestionnaire("row_level", dd, config['dataset'][category].get('filename'))

                    data_functions.append(row_composition)
                    process_functions.append(row_init)

        if len(data_functions) > 0:
            
            filename = WriteWhistleFile(whistle_src_dir, data_functions, process_functions)
            print(f"File created: {filename}")
        else:
            print(f"No file created this time")
