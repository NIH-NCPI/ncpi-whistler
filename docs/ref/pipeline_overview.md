# Pipeline Overview
NCPI Whistler moves data across multiple stages which are described below. 

## Extraction
The extraction phase takes each of the configuration's "data tables" and their data dictionaries and produces a JSON file which will serve as the input for the whistle execution. This file is a standard JSON object with the following keys: study, code-systems and harmony along with an entry for each of the tables defined in the configuration file. 

### Extraction Output Blocks
#### study
The study entry is an object that contains information pulled from the study configuration file such as the study id, title, description, url, publisher, etc. 

#### code-systems
The code-systems entry is an array of objects that contains details about the local CodeSystems that were pulled from the data-dictionaries either as lists of column names from the tables themselves, or enumerated values from the values lists. These include information about the url (that should become the CodeSystem's system), table name each of the values as a list of objects with the properties, code and description. 

#### harmony
The ConceptMaps provided to Whistle during the FHIR production stage are not suitable for being loaded into FHIR. For one, they include *self references* which can be helpful when taking a value from a dataset and identifying using the accompanying descriptive text that was provided by the data-dictionary. In addition to this, there is also the fact that the *local code system* values in the official harmony files aren't necessarily proper URLs, which makes them illegal to use as system values in FHIR. So, Whistler creates the input necessary for complete ConceptMaps to be constructed and loaded into FHIR. These ConceptMaps are one of the FHIR Resources that Whistler's machine generated whistle code can generate. 

#### Table Data - One Table, One Entry
Each table will have it's own entry in the root object, named after the its dataset key from the study configuration. Those entries are simple lists of objects, each containing a single row of data. The keys in these objects are *modified versions* of the variable names and the data will be the value from that variable's cell for the given row. The modifications to the variable names are simply to drop case and replace any whitespace with underscores. This allows whistle code to access the properties as variables yet the origin remains clear. 

#### Exceptions to One Table, One Entry
When defining a data table inside the study configuration, the ETL author has a couple of options for how the table is written to the whistle input file. 

##### group_by
For situations where multiple rows may be encountered in the original table that should be considered together during whistle projection, the table property, *group_by* can be added with the value being one or more variable names to be grouped by (multiple columns should be specified as one value where each variable name is separated by a column). The behavior will be similar to the SQL GROUP BY clause.

For each of the *grouped* rows, a single entry in the table will be created with the *grouped by* columns being keys along with an additional key being the variable names that were part of the grouping separated by a colon. This will point to an object with a single key named *content" whose value is a list. This list contains the rest of each of the rows that matched the particularly grouping. 

##### embed
A more common issue is when a component of one table exists as rows in another table. For example, one data table contains specimen details and another contains the all of the aliquots associated with those specimen data. Whistle authors could do matching to collect those aliquots when constructing the specimen resources, but that is very slow in whistle. A much more efficient way to go about this would be to embed the aliquots within the specimen table itself. 

To embed a table, such as the aliquot table in the example above, inside another table, the property *embed* can be used. It must have two properties:
* dataset - which is the name of the table in which this particular table is to be embedded in 
* colname - which is the variable name(s) which establishes unique JOIN criteria. If more than one variable name is necessary, provide them all as a comma separated list

### Other Extractions Performed
In addition to the whistle input JSON file, the harmony files are parsed and converted into valid ConceptMaps that are going to be passed to whistle during projection. 

Unlike the whistle input JSON file, harmony conversions happen with each run, regardless of the timestamp on the dependent file(s). This is necessary since one can have a common harmony file that is used for related but different studies. Because the ConceptMap used during whistle projection contains mappings to local CodeSystems, and those CodeSystems contain the study ID as part of the formal System URL, those must be built every time to avoid confusing references to other study's CodeSystems. 

## Transformation
NCPI Whistler handles transformation in two ways: Harmony and Whistle Code. 

The Harmony file(s) is a simple CSV file listing the values from the local dataset to public ontologies. Technically, these are transformed into a FHIR ConceptMap which is accessible by whistle code, but from a documentation standpoint, those transformations in CSV Format are very simple to understand and make for a clear record of what was mapped to which public ontologies. 

The Whistle code, on the other hand, is just a tiny a bit more complex. In many ways, it looks like functions whose bodies are just JSON objects. As such, Whistle Code is generally very easy to read and write for anyone who is familiar with JSON and FHIR. The variables from the dataset are projected into the resource output without any decoration. So, it's very easy for the reader to identify where the data comes from if it's a direct assignment from the dataset. 

The snippet below is actual Whistle code that produces a Patient resource from a data table: 
```
def Participant(study, subject) {
    meta.tag[]: StudyMeta(study);
    identifier[]: Key_Identifier(study, "Patient", subject.participant_id);
    identifier[0].use: "official";
    gender (if subject.sex ~= "."): HarmonizeAsCode(subject.sex, "Sex");
    extension[]: RaceExtension(subject);
    extension[]: EthnicityExtension(subject);
    resourceType : "Patient"
}
```

The above function accepts two objects, a study and a subject and builds the Patient resource using data from those two. The HarmonizeAsCode function utilizes the builtin whistle Harmony system transform whatever the subject's sex is into the ontological terms we specified in the Harmony CSV. Race and Ethnicity are a bit more complex, since they are extensions, so their functionality has been moved into a function to make things a bit more readable. But, for the most part, they too will employ the same Harmonize functions along with the extract scaffold bits required to construct those extensions. 

What is nice about this, though, is the readability. One can easily see which columns the identifier and sex came from. 

## Load
Loading data into FHIR is the final step in the ETL process and can also be handled by NCPI Whistler. Users can configure as many endpoints as is appropriate for the data they are working on including any authentication details, URL, etc. These configurations are given a shorthand name, such as 'dev', 'local', 'qa', etc which can be specified on the command line making it painless to load into 'dev', verify that the changes are as expected and then progress into 'qa' then 'prod' as appropriate. Unless specified otherwise, Whistler only projects those final FHIR resources if a change to one of its dependencies is detected, making it generally quite efficient. 

Loading can be performed in whole or selectively depending on the users needs. If the only changes made were to the Specimen resources, the user can limit the loading to only consider Specimen FHIR Resources. 

Referential integrity is maintained by querying the local server for identifiers for each of the FHIR Resource types. This does add a few minutes to load process up front, but it's done in bulk and ensures that whistler is aware of all resources associated with the current study even if those changes are made by someone else. Resources can be loaded using threads, making the load times entirely dependent on the configuration of the FHIR server. We've been able to load large numbers of resources into Google's Healthcare API in only a few minutes once the request-per-minute limit was significantly increased. 
