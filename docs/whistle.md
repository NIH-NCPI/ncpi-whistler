# Whistle - Google HCLS Data Harmonization
[Whistle](/#/whistle) is google's Data Transformation Language which can be used to transform arbitrary JSON objects into FHIR compliant JSON objects. In addition to providing a language specific to data transformations, it also integrates nicely with [FHIR ConceptMaps](http://hl7.org/fhir/R4/conceptmap.html) making it a great choice for use in harmonizing the various research datasets into a much more consistent, FHIR representation. 

## Whistle Installation
Whistle is an independent software application with its own requirements which must be installed prior to being built. Please note that some of these, such as Java, may already be installed or can be installed in other ways than described below. 

* [Golang 1.13 or higher](https://go.dev/doc/install)
* [Java SDK 8 or Higher](https://openjdk.org/install/)
* [Protocol Buffers v3.11.4 or higher](https://github.com/protocolbuffers/protobuf/releases/tag/v3.11.4)
* [clang 11.0.1](https://clang.llvm.org/get_started.html)

Once these dependencies have been installed, the user should clone the [repository](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization) and then run the repository's build_all.sh script. 

## Whistle only Docker Image
I have also put together docker image and shell script for those who prefer not to install the libraries above (or have difficulty installing them on their target platform). This can be found [here](https://github.com/NIH-NCPI/dockerized-whistle)

