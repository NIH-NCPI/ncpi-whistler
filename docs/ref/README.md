# NCPI Whistler Reference Guide
NCPI Whistler is a python based pipeline designed to transform research data into FHIR Resources. At its heart, Whistler uses Alphabet's Whistle language to do the transformations. This provides two key benefits for our needs as transforming Research Data:

- Alphabet's [Whistle](/whistle) language provides the ability to build FHIR resources from source data in a very transparent way. The code itself looks very similar to actual FHIR resources and the extracted data being inlined in a suitably conspicuous way to allow easy reference to where any given value originated. 
- [Harmony Files](/harmony) are simple CSV files that get transformed into FHIR ConceptMaps which are then passed to whistle during the transformation. The mappings produced by these harmony files are employed by whistle code to perform transformations and can also serve as a record of all data harmonizations employed by a given ETL process.

NCPI Whistler provides an end-to-end solution for converting the research input files into a format suitable for passing to Whistle code, building the ConceptMaps for proper harmonization, running whistle and loading the output into a FHIR server.  

## Installing Whistler
NCPI Whistler is a python 3 application. As such, it should run on any modern system. 

### Prerequisites
- Python 3.7 or later
- [Whistle](/#/whistle)
- Various Python libraries (which should be automatically installed when using pip)
  - PyYAML
  - packaging
  - ncpi_fhir_client
  - rich

### Basic Installation
The easiest way to install the application itself is to clone this repository, cd into the repository's root directory and run the following command: 

``pip install .``

### Alternate Installation via Docker
I have put together a docker image and a set of scripts that allows users to simply build a docker image and copy some basic shell scripts that handle the docker calls for you, allowing you to run the exact same commands as you would using the regular python application suite. That can be found [here](https://github.com/NIH-NCPI/dockerized-whistler)