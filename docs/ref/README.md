# NCPI Whistler Reference Guide
NCPI Whistler is a python based pipeline designed to transform research data into FHIR Resources. At its heart, Whistler uses Alphabet's Whistle language to do the transformations. This provides two key benefits for our needs as transforming Research Data:

- Alphabet's [Whistle](/whistle) language provides the ability to build FHIR resources from source data in a very transparent way. The code itself looks very similar to actual FHIR resources and the extracted data being inlined in a suitably conspicuous way to allow easy reference to where any given value originated. 
- [Harmony Files](/harmony) are simple CSV files that get transformed into FHIR ConceptMaps which are then passed to whistle during the transformation. The mappings produced by these harmony files are employed by whistle code to perform transformations and can also serve as a record of all data harmonizations employed by a given ETL process.

NCPI Whistler provides an end-to-end solution for converting the research input files into a format suitable for passing to Whistle code, building the ConceptMaps for proper harmonization, running whistle and loading the output into a FHIR server.  

# Installing Whistler
To learn more about installing whistler, please see the [installation instructions](/installation).

# Table of Contents
* [FHIR Hosts File](/ref/fhir_hosts)
* [Whistle Harmony Files](/ref/harmony_files)
* [Project Config File](/ref/project_config)
* [Data Dictionary](/ref/data_dictionary)
* [Writing Whistle Projections](/ref/whistle_projections)
* [Pipeline Overview](/ref/pipeline_overview)
* [Application Suite](/ref/suite)