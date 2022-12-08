# ncpi-whistler
NCPI Whistler is a python based pipeline designed to transform research data into FHIR Resources. It employs alphabet's [Whistle](/whistle) language to make the transformation process transparent and easy to understand for anyone familiar with JSON and the core FHIR model. The pipeline provides an automated system to walk the data from CSV, into Whistle, through to being loaded into a FHIR server. Several FHIR authentication methods are supported including Basic Auth, Open Auth 2, GCP Service Account using an easily extendable authentication system. Resource compilation and loading are both modular, enabling minor fixes to be performed in a reasonable amount of time. 

# NCPI Whistler Overview
NCPI Whistler relies on a combination of YAML configuration files, study Data Dictionaries and project specific whistle code to extract, transform and load the data into FHIR. 

## FHIR Hosts
The FHIR Hosts file is a simple YAML file that contains the endpoints where the data can be loaded. This file provides all the details necessary to authenticate the client against the target server and load the data. Several different authentication schemes are currently supported. 

Read more about writing fhir_hosts file as well as the supported authentication methods, click [here](/ref/fhir_hosts).

## Whistle Projections
Whistle Projections are the whistle 'templates' which are compiled into FHIR resources during pipeline execution. These files should reside within a single directory within the working directory from which whistler is to be run and should contain all whistle code necessary to support the study's transformation into FHIR. 

For related studies with identical or nearly identical input format, it is recommended to reuse the same projections with a single configuration for each distinct study. Each of these configurations would pass the appropriate data to the whistle application during processing, resulting in correct transformations using the same underlying code. 

Read more about creating [whistle projections](/ref/whistle_projections). 

## Harmony Files
A key component of Whistle is the use of FHIR ConceptMaps to translate local study values into public ontological terms provided by the ETL authors. The harmony files used by NCPI Whistler are simple CSV files that get transformed into ConceptMaps during pipeline execution and subsequently passed on to whistle for use during the transformation process. This approach provides a clear log of all data transforms employed by the ETL process and makes reading the whistle code clear. 

Harmony files are specified as part of the project configuration. They can be common for many studies using the same whistle projects or not, depending on the specific needs of the study itself. 

Read more about writing [harmony files](/ref/harmony_files).

## Project Configuration
Project configuration is contained within a YAML file which describes various study attributes, paths to each of the source files to be processed and their corresponding data-dictionaries, path to harmony file(s) and module activation.

Read more about creating [project configuration files](/ref/project_config).

## Study Data Dictionary
Each study table should have a corresponding data-dictionary which informs the application of each variable name, its type, a human friendly description and a semi-colon separated list of acceptable values for variables of that type. These files are necessary for building the Study Metadata resources but also are used to help inform other aspects of the pipeline about the contents of the study tables themselves. 

Read more about how to inform NCPI Whistler of the individual [data dictionaries](/ref/data_dictionary) used in the study. 

## Pipeline Overview
While the transformation itself can be run as a single command, there are a number of steps involved in ETL pipeline leading up the final data that gets loaded into FHIR. Each of these steps is guarded against unnecessary processing based on dependency timestamps which helps to prevent redoing work that isn't necessary. 

Read more about the [process](/ref/pipeline_overview) and how whistler moves data through the different steps. 

# Application Suite
NCPI Whistler is actually a suite or programs designed to perform different aspects of the FHIR transformation and load process. These tools include:
  * play - The main script that runs the pipeline to create harmony concept maps, extracts data from CSV, runs whistle and (optionally) loads data into a FHIR server. 
  * delfhir - A tool that is designed to help mass delete FHIR resources.
  * whistle generation - There are several tools designed to initialize a whistle projection, create [NCPI IG metadata](https://nih-ncpi.github.io/ncpi-fhir-ig/study_metadata.html) resources based on a study's actual data-dictionary as well as functions that are recommended for creating identifiers, filtering items out of lists, etc. 

# Reference Manual
There is a fairly complete [reference manual](/ref/) available that covers details about the various aspects of creating a whistler project. 


