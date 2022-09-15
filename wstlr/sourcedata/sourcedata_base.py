
srcd_observation_def_base = """
def BuildObsComponentString(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
    valueString: value;
}

def BuildObsComponentCategorical(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
    valueCodeableConcept.coding[]: HarmonizeMapped(value, varname);
}

def BuildObsComponentInteger(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
    if ($Type(value)="number") {
        valueQuantity.value: $ParseInt(value);
    } else {
        valueString: value;
    }
}

def BuildObsComponentQuantity(study, table_name, varname, required value) {
    code.coding: HarmonizeMapped(varname, table_name);
        
    if ($Type(value)="number") {
        valueQuantity.value: $ParseFloat(value);
    } else {
        valueString: value;
    }
}
"""

srcd_questionnaire_def_base = """
def BuildQuestionnaireURL(study, table_name) {
    $this: $StrCat(study.identifier-prefix, "/data-dictionary/rl-questionnaire/", study.id, "/", $ToLower(table_name));
}

// There are a few ways to handle values that don't work as numbers, so 
// we'll just make a function to use them so it will be easier to change. 
def QuestionnaireNotNumber(value) {
    valueCoding: BuildCoding("not-a-number", "Not a Number (NaN)", "http://terminology.hl7.org/CodeSystem/data-absent-reason");
    // valueString: value
}

// Questionnaire Response Item helpers
def ReportQuestionnaireItemBasic(study, varname, text, value) {
    linkId: varname;
    text: text;
    answer[].valueString: value;
}
def ReportQuestionnaireItemCategorical(study, varname, text, code) {

    var coding: HarmonizeSelectByPrefix(study, code, varname);
    linkId: varname;
    text: text;
    if (coding?) {
        answer[].valueCoding: coding;

    } 
}

def ReportQuestionnaireItemInteger(study, varname, text, required value) {
    linkId: varname;
    text: text;
    if ($Type(value)="number") {
         answer[].valueInteger: $ParseInt(value);
    } 
}

def ReportQuestionnaireItemQuantity(study, varname, text, required value) {        
    linkId: varname;
    text: text;

    if ($Type(value)="number") {
        answer[].valueQuantity.value: $ParseFloat(value);
    } 
}

// Questionnaire Item Construction
def BuildQuestionnaireItemBasic(study, table_name, varname, text, datatype) {
    linkId: varname;
    text: text;
    type: $ToLower(datatype);
}
def BuildQuestionnaireItemCategorical(study, table_name, varname, text, datatype, url) {
    linkId: varname;
    text: text;
    type: "choice";
    answerValueSet: url;
}

"""