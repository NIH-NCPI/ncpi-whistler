# NCPI Whistler Applications
All of the command line tools provided by Whistler support the POSIX --help argument which provides a comprehensive list of arguments and their descriptions. 

## play
play is the main pipeline script that extracts data from dataset and harmony CSV files, transforms those into whistle input JSON files, executes Whistle and optionally loads that data into the designated FHIR server. 

(more info to come)

## delfhir
delfhir provides a simple interface to drop resources from a FHIR server based either by study Meta.tag or IDs found in a previous load's id log. The script does support restricting deletions to specific resource types as well as an entire study. 

(more info to come)

## igload
Pull definitions from published IGs or from local IG and load them into FHIR. This application allows the user to specify one or more IGs to load data from and provides options to restrict which resources to be loaded. 

Read more about [igload](/ref/igload).

## Whistle Generation 
There are a few scripts dedicated solely to generating some general purpose whistle code. 

Each of these scripts can be run more than once to incorporate changes that have been made to the data-dictionary, however, do be warned that any user edits to the files that are created will be lost. 

### init-play
This script initializes the whistle projection directory adding a number of files that contain helpful functions that are believed to be suitable for all projects. In addition to this, the script also adds the functions necessary to construct FHIR CodeSystems suitable to capture the terms associated with the study's data-dictionary such as each table's variable names, various enumerated lists associated with some of the variables, etc. 

(more info to come)

### buildsrcobs 
This script will generate the functions necessary to build the source data representations as FHIR Observation resources. Each row from one of the data tables will be represented as a single FHIR Observation with each variable being recorded as a separate component within the components property. 

(more info to come)

### buildsrcqr 
This script will generate the functions necessary to build source data representations as Questionnaires and QuestionnaireResponses. Each row will be represented as a QuestionnaireResponse that conforms to the Questionnaire which is defined by a particular table's layout. 



(more info to come)


