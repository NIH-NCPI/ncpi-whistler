
rl_observation_def_base = """
def BuildObsComponentString(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
    valueString: value;
}

def BuildObsComponentCategorical(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
    valueCodeableConcept.coding[]: Harmonize(value, varname);
}

def BuildObsComponentInteger(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
    valueInteger: $ParseInt(value);
}

def BuildObsComponentQuantity(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
    valueQuantity.value: $ParseFloat(value);
}
"""

rl_questionnaire_def_base = """
def BuildQuestionnaireURL(study, table_name) {
    $this: $StrCat(study.identifier-prefix, "/data-dictionary/rl-questionnaire/", study.id, "/", $ToLower(table_name));
}

// Questionnaire Response Item helpers
def ReportQuestionnaireItemBasic(study, varname, text, value) {
    linkId: varname;
    text: text;
    answer[].valueString: value;
}
def ReportQuestionnaireItemCategorical(study, varname, text, code) {

    var coding: HarmonizeMappedFirst(code, varname);

    if (coding?) {
        answer[].valueCoding: coding;
        linkId: varname;
        text: text;
    } else {
        answer[].valueString: code;
    }
}

// Questionnaire Item Construction
def BuildQuestionnaireItemBasic(study, table_name, varname, text, datatype) {
    linkId: varname;
    code: Harmonize(varname, table_name);
    text: text;
    type: $ToLower(datatype);
}
def BuildQuestionnaireItemCategorical(study, table_name, varname, text, datatype, url) {
    linkId: varname;
    code: Harmonize(varname, table_name);
    text: text;
    type: "choice";
    answerValueSet: url;
}

"""