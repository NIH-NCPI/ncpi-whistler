# Harmony Files
Whistle officially support the use of FHIR ConceptMaps for translating terms found in the original data to public terms. These maps must be defined by the one running the ETL, however. To make this task easier, NCPI Whistler provides a simple CSV format that can be populated by domain experts and converted during the pipeline execution. This provides a clear mechanism for documenting transformations and separates those from the actual whistle code which may be overwhelming for those unfamiliar with reading JSON files. 

Each data table can have a distinct harmony file, but it's not uncommon for the entire dataset to use the same file. The choice to separate is really up to the preferences of those managing the whistler projects. The harmony file's name is specified as the value to the dataset's *code_harmonization* property in the configuration file. All harmony files must be present in the directory specified by the configuration property, *code_harmonization_dir*. 

# Harmony File Column Names
## local code
This references the value that will be found inside the dataset that is to be translated.

## text
This is a human readable label associated with that value. For instance, if the gender were encoded as *1* and *2*, those encoding values would appear in the local code, and the human friendly values, *male* and *female* would appear in the *text* column. 

## table_name
This column is helpful for Whistle to account for assignments when generating a formal ConceptMap which can be stored in FHIR to represent transformations being made. In general, this should be the key in the configuration's dataset array associated with the variables source table. 

## parent_varname
Similar to table_name above, this is used for accounting purposes to help represent the transformations performed as a ConceptMap in FHIR. Its contents should contain the variable name for values that are enumerated types. 

## local code system
This is some sort of key that is used differentiate the *local code* value from other codes. In general, this would be the table name or the parent variable name, depending on the type of data in *local code*.

## code
This is the code that the *local code* is to be translated to, generally it would be from one of the many public ontologies. 

## display
This is the label or display value associated with the *code* property. Generally, this would be the description from the public code's ontological entry. 

## code system
This is the formal URI associated with the ontology from which the public *code* is a part of. 

## Other Columns - Such as Comment
The columns above are all required, though, they some may be left empty. However, if there is value in providing comments or other details, users are welcome to add as many columns as they wish. Please be aware, though, that these will ignored during processing and will not end up as part of the Whistle Input and thus, can not be added to any FHIR resource by way of NCPI Whistler's normal functionality. 

# Handling Multiple Mappings for a Single Value
For values that map to entries in multiple ontologies, additional rows with the same *local code* are permitted (and, in may cases, expected). We strongly recommend against squeezing multiple values into the *code*, *display* and *code system* properties even if it is possible to correctly tease them out using whistle functionality. This makes the harmony file less readable and will likely make some of the automated functionality of Whistler less effective. 


