# Load IG Definitions Into FHIR
In order to validate FHIR resources against profiles and other definitions referenced in a FHIR IG, the server must have access to those resources. Whistler provides a simple tool to allow the user to load definitions from a FHIR IG either online or present on their local filesystem. 

In order for igload to work, the IG must have been built using HL7s publisher application and must have the file, definitions.json.zip accessible to the application. The application relies on a simple YAML config and allows for the use of Whistler study configuration files to enable one to easily direct the resources to the appropriate servers (TBD). 

## Usage
```
usage: igload [-h] [--host {tutorial}] [-r RESOURCE] [-x EXCLUDE] [-c CONTENT]
              [--generate-default] [--sleep-time SLEEP_TIME] [--version]

Load whistle output file into selected FHIR server.

options:
  -h, --help            show this help message and exit
  --host {...}          Remote configuration to be used to access the FHIR server. If no
                        environment is provided, the system will stop after generating the
                        whistle output (no validation, no loading)
  -r RESOURCE, --resource RESOURCE
                        When loading resources into FHIR, this indicates a resourceType that
                        will be loaded. --resource may be specified more than once.
  -x EXCLUDE, --exclude EXCLUDE
                        When loading resources into FHIR, any resources matching any exclude
                        entry will be skipped. Exclusions match case.
  -c CONTENT, --content CONTENT
                        YAML File with details about the IG to load into FHIR
  --generate-default    When used, a default configuration will be dumped to std:out
  --sleep-time SLEEP_TIME
                        Number of seconds to sleep between deleting pre-existing resources
                        and subsequent loading. If you have very large vocabularies as part
                        of your IG(s), then it may be helpful to increase this value.
  --version             Return the version number associated with the application.
  ```
Like the others in the Whistler suite, this tool requires the [FHIR Hosts File](/ref/fhir_hosts) file. That defines which hosts are available (and where the options for --host are defined). 

The option, *--content* specifies the YAML file in which the IG sites are defined. There can be multiple sites provided. The option, *--generate-default* will dump the default NCPI FHIR IG configuration to *standard out*, allowing users to simply redirect this output to a file named as they choose. They can then add other IG sites as well for situations where they have special terminologies or profiles that they wish to load that aren't are a part of the NCPI FHIR IG. 

The flag, *--sleep-time* allows the user to increase the delay between deletes and loads (the tool first attempts to delete any resources that might be old versions of the resources being loaded). 

The flags, *--exclude* and *--resource* allow the user to restrict which definitions to load into the FHIR server. These flags can be used multiple times each. If *--resource* is not set, the application uses whatever is configured in the current module of the specified configuration. 