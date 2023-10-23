from pathlib import Path
from ncpi_fhir_client.fhir_client import FhirClient
from yaml import safe_load
from wstlr import get_host_config
from wstlr.igload import ig_source, file_source
import zipfile
import requests
from tempfile import TemporaryFile
import json
from time import sleep

from argparse import ArgumentParser, FileType
import sys
import pdb


from rich import print 

__version__ = "0.1.0"

def test_exclusion(filename, exclusion_list):
    if exclusion_list is not None:
        for excl in exclusion_list:
            if excl in filename:
                return True
    return False

def exec():
    host_config = get_host_config()
    env_options = sorted(host_config.keys())
    
    parser = ArgumentParser(
        description="Load whistle output file into selected FHIR server."
    )
    parser.add_argument(
        "--host",
        choices=env_options,
        help=f"Remote configuration to be used to access the FHIR server. If no environment is provided, the system will stop after generating the whistle output (no validation, no loading)",
    )
    parser.add_argument(
        "-r", 
        "--resource",
        type=str,
        action='append',
        help="When loading resources into FHIR, this indicates a resourceType that will be loaded. --resource may be specified more than once."
    )
    parser.add_argument(
        "-x",
        "--exclude",
        type=str,
        action='append',
        help="""When loading resources into FHIR, any resources matching """
             """any exclude entry will be skipped. Exclusions match case. """
    )
    parser.add_argument(
        "--force-overwrite", 
        action='store_true',
        help="Replace resources that are already loaded in the target FHIR server."
    )
    parser.add_argument(
        "-c",
        "--content",
        type = FileType('rt'),
        help="YAML File with details about the IG to load into FHIR"
    )
    parser.add_argument(
        "--generate-default",
        action='store_true',
        help="When used, a default configuration will be dumped to std:out"
    )
    parser.add_argument(
        "--sleep-time",
        type = int,
        default=5,
        help = """Number of seconds to sleep between deleting pre-existing """
             """resources and subsequent loading. If you have very large """
             """vocabularies as part of your IG(s), then it may be helpful """
             """to increase this value. """
    )
    parser.add_argument(
        "--version",
        action='store_true',
        help="Return the version number associated with the application. "
    )
    args = parser.parse_args(sys.argv[1:])

    if args.version:
        print(f"{Path(__file__).parent.name} v{__version__}")
        sys.exit(0)

    if args.generate_default:
        print((Path(__file__).resolve().parent / "templates" / "ncpi.yaml").open('rt').read() + "\n")
        sys.exit(0)

    if args.content is None:
        response = input("No content file provided, do you want to load the default IG site? (Y/n) ")
        if response == "" or response.lower()[0] == 'y':
            args.content = open(str(Path(__file__).resolve().parent / "templates" / "ncpi.yaml"), 'rt')
        else:
            sys.stderr.write("No site configuration provided. Unable to continue")
            sys.exit(1)
    content = safe_load(args.content)

    fhir_client = FhirClient(host_config[args.host], idcache=None)
    print(f"Destination host: {fhir_client.target_service_url}")

    for key in content:
        resources = {}

        if content[key]['source_type'] == "IG":
            resources = ig_source.load_resources(content[key])
        elif content[key]['source_type'] == "FILES":
            resources = file_source.load_resources(content[key])

        resource_list = args.resource
        if resource_list is None:
            resource_list = content[key]['resources']
            if type(resource_list) is str:
                resource_list = [x.strip() for x in resource_list.split()]

        exclusion_list = args.exclude
        if exclusion_list is None:
            exclusion_list = content[key].get('exclude-wildcards')
            if type(exclusion_list) is str:
                exclusion_list = [x.strip() for x in exclusion_list.split()]

        excluded_list = []
        # First, let's try deleting any that may already exist
        deleted_items = []
        #pdb.set_trace()
        if args.force_overwrite:
            for fn,data in resources.items():
                if data['resourceType'] in resource_list and not test_exclusion(fn, args.exclude):
                    response = fhir_client.delete_by_query(data['resourceType'], qry=f"url={data['url']}")

                    if len(response) > 0:
                        if type(response) is dict:
                            response = [response]

                        for resp in response:
                            try:
                                print(f"Deleting {fn} - {resp['status_code']}")
                                if fn not in deleted_items:
                                    deleted_items.append(fn)
                            except:
                                print(resp)
                                print(len(resp))
                                pdb.set_trace()
            if len(deleted_items) > 0:
                print(f"Sleeping to give the backend time to catchup")

                sleep(args.sleep_time + len(deleted_items))

        # Iterate over the list and load them one at a time
        for fn,data in resources.items():
            if (data['resourceType'] in resource_list or fn in resource_list) and not test_exclusion(fn, exclusion_list):
                response = fhir_client.load(data['resourceType'], data, skip_insert_if_present=not args.force_overwrite)
                if type(response) is dict:
                    response = [response]
                
                for resp in response:
                    if resp['status_code'] < 300:
                        print(f"Loading {fn} - {resp['status_code']}")
                    else:
                        print(f"An error occurred loading {fn}")
                        print(resp['status_code'])
                        if 'issue' in resp:
                            print(resp['issue'])
                        else:
                            print(resp)
                        pdb.set_trace()
            else:
                excluded_list.append(fn)

        print("Files Loaded: " + ", ".join(sorted(resources.keys())))
        print("Files Excluded: " + ", ".join(sorted(excluded_list)))

    print("""   
          \nPlease note that for some FHIR Servers, we've noticed that """ 
          """loading large vocabularies can take quite some time before the """
          """server is ready to use them and any changes made afterward. """)
