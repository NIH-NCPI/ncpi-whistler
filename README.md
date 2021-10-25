# ncpi-whistler
Data Harmonization Pipeline For NCPI Data Into FHIR

[Whistle](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization) is google's Data Transformation Language which can be used to transform arbitrary JSON objects into FHIR compliant JSON objects. In addition to providing a language specific to data transformations, it also integrates nicely with [FHIR ConceptMaps](http://hl7.org/fhir/R4/conceptmap.html) making it a great choice for use in harmonizing the various research datasets into a much more consistant, FHIR representation. 

In order to employ whistle, however, the data must be formatted as JSON and any code transformations must be provided as ConceptMaps. NCPI-Whistler aims to do just that as well as aide users in the delivery of those FHIR resources into the FHIR server of choice. 

## Extraction
Whistle transform JSON into JSON. As such, the input file must be JSON. NCPI-Whistler provides a simple YAML configuration for the dataset which is used to extract data from the CSV format into JSON objects suitable for use by Whistle functions. The goal here will be to provide a rich system to support many different data layouts and configurations. 

## Transformation
NCPI-Whistler will convert simple to use CSV files into valid FHIR ConceptMaps which are used by Whistle to harmonize the various codes used throughout the dataset. When done properly, these CSV file(s) will provide a reasonably comprehensive mapping from the data values found inside the dataset to appropriate common vocabularies such as LOINC, SNOMED, HPO, Mondo, etc. These files should be easy to edit and update and will be automatically employed within the whistle code itself. 

## LOAD
Loading and validating data into a FHIR server isn't particularly difficult, but NCPI-Whistler aims to provide a complete pipeline for the process and therefor, will provide the ability to load the transformed output into the FHIR server of choice. 
