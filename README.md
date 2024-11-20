# NOVEMBER 2024 - Breaking Changes
Please note that version 0.3.0 introduces breaking changes to the YAML configuration: 
Projections are not modular and require entries for each of the following: study, dataset, harmonized. Inside, you can create a directory, current (the default choice for projection version), and move your projections into that directory. You can then create an entry for each of the modules that point to the same projection library: 

Example: 
```
projections: 
  study: projector
  dataset: projector
  harmonized: projector
```

You should move your entry point (typically _entry.wstl) into the projector directory (whistler will look to the version's parent directory for the file)


# NCPI Whistler
NCPI Whistler provides a complete pipeline to transform research data tables into FHIR resources loaded into a FHIR Server using a combination of standard Python scripting, [Whistle](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization) and the standard FHIR Rest API for loading data into the target FHIR server. 

Whistle is google's Data Transformation Language which can be used to transform arbitrary JSON objects into FHIR compliant JSON objects. In addition to providing a language specific to data transformations, it also integrates nicely with [FHIR ConceptMaps](http://hl7.org/fhir/R4/conceptmap.html) making it a great choice for use in harmonizing the various research datasets into a much more consistant, FHIR representation. 

In order to employ whistle, however, the data must be formatted as JSON and any code transformations must be provided as ConceptMaps. NCPI-Whistler aims to do just that as well as aide users in the delivery of those FHIR resources into the FHIR server of choice. 

## Installing Whistler
Whistler can be installed as a native python application or as a docker container. Please see the manual for [installation instructions](https://nih-ncpi.github.io/ncpi-whistler/#/installation).

## Whistler Walk-through
If you are curious about whether Whister is right for your project, there is a [tutorial](https://nih-ncpi.github.io/NCPI-Whistler-Tutorial) available to help you get started. 

## Extraction
Whistle transforms JSON into JSON. As such, the input file must be a JSON file. NCPI-Whistler provides a simple YAML configuration for the dataset which is used to extract data from the CSV format into JSON objects suitable for use by Whistle functions. The goal here will be to provide a rich system to support many different data layouts and configurations. 

## Transformation
NCPI-Whistler will convert simple to use CSV files into valid FHIR ConceptMaps which are used by Whistle to harmonize the various codes used throughout the dataset. When done properly, these CSV file(s) will provide a reasonably comprehensive mapping from the data values found inside the dataset to appropriate common vocabularies such as LOINC, SNOMED, HPO, Mondo, etc. These files should be easy to edit and update and will be automatically employed within the whistle code itself. 

## Load
Loading into and validating data against a FHIR server isn't particularly difficult, but NCPI-Whistler aims to provide a complete pipeline for the process and therefor, will provide the ability to load the transformed output into the FHIR server of choice. 

The load process is entirely modular allowing one to load chunks of data based on the whistle output organization (called modules which are defined by the Whistle projection's author) as well as by specific resource type. This level of control allows the user to load only the resources that should be loaded to save time and, potentially, transaction costs when loading data into a cloud based FHIR server. 

# Application Suite
NCPI Whistler is actually a suite or programs designed to perform different aspects of the FHIR transformation and load process. These tools include:
  * play - The main script that runs the pipeline to create harmony concept maps, extracts data from CSV, runs whistle and (optionally) loads data into a FHIR server. 
  * delfhir - A tool that is designed to help mass delete FHIR resources.
  * whistle generation - There are several tools designed to initialize a whistle projection, create [NCPI IG metadata](https://nih-ncpi.github.io/ncpi-fhir-ig/study_metadata.html) resources based on a study's actual data-dictionary as well as functions that are recommended for creating identifiers, filtering items out of lists, etc. 

## play
play is the main pipeline script that orchestrates the entire process of converting CSV files into resources loaded into a FHIR server. In order for play to run, you must have a properly defined configuration YAML file which describes the study be processed as well as the files that make up the study. Each of the CSV files must have a matching data dictionary file enumerating the column names, basic type and any enumerated values if the field is a "drop down" style variable. 



