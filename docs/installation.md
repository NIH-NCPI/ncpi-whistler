# Installing Whistler
NCPI Whistler is a python application. There have been some minor changes to support 3.10, but it runs fine on version 3.7 or later. 

## Prerequisites
- Python 3.7 or later
- [Whistle](/#/whistle)
- Various Python libraries (which should be automatically installed when using pip)
  - PyYAML
  - packaging
  - ncpi_fhir_client
  - rich
  - jinja2

The easiest way to install the application itself is to clone this repository, cd into the repository's root directory and run the following command: 

``pip install .``

### Whistle Installation
A key component of Whistler is the Alphabet's Whistle application, which has its own requirements (please note that some of these, such as Java, may already be installed or can be installed in other ways that described below): 

* [Go](https://go.dev/doc/install)
* [Java SDK 8 or Higher](https://openjdk.org/install/)
* [Protocol Buffers v3.11](https://github.com/protocolbuffers/protobuf/releases/tag/v3.11.4)
* [clang 11](https://clang.llvm.org/get_started.html)

I have also put together docker image and shell script for those who prefer not to install the libraries above (or have difficulty installing them on their target platform). This can be found at the [dockerized whistle](https://github.com/NIH-NCPI/dockerized-whistle) repo. Please note this provides only the whistle application itself along with a helper script to handle the underlying docker calls for you. When the help script is available in your PATH, it will work seamlessly with a natively installed Whistler as described above. 

### Alternate Installation via Docker
For those who prefer not to install Whistler directly onto their systems, I have created a Docker Image which contains Whistle and NCPI Whistler along with some helper scripts to make running whistler a bit more convenient. That can be found at the [dockerized whistler](https://github.com/NIH-NCPI/dockerized-whistler) repo. 