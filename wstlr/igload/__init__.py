from pathlib import Path
from ncpi_fhir_client.fhir_client import FhirClient
from yaml import safe_load
from wstlr import get_host_config
import zipfile
import requests
from tempfile import TemporaryFile
import json
from time import sleep

from argparse import ArgumentParser, FileType
import sys
import pdb

__version__ = "0.1.0"

def test_exclusion(filename, exclusion_list):
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
    for key in content:
        resources = {}

        resource_list = args.resource
        if resource_list is None:
            resource_list = content[key]['resources']
            if type(resource_list) is str:
                resource_list = [x.strip() for x in resource_list.split()]

        exclusion_list = args.exclude
        if exclusion_list is None:
            exclusion_list = content[key]['exclude-wildcards']
            if type(exclusion_list) is str:
                exclusion_list = [x.strip() for x in exclusion_list.split()]

        # Recorded filenames that were excluded
        excluded_list = []

        f = None

        ig_source = ""

        if 'url' in content[key]:
            # Grab the definition zip file from the IG website
            url = f"{content[key]['url']}/definitions.json.zip"
            ig_source = url
            response = requests.get(url)

            # Open it up using a Temporary File
            f = TemporaryFile()
            f.write(response.content)
            f.seek(0)
        elif 'path' in content[key]:
            # If we have a path, then we just open the file as usual
            ig_source = content[key]['path']
            f = (Path(content[key]['path']) / "output/definitions.json.zip").open('rb')
        else:
            sys.stderr("ERROR: Each module MUST contain either a 'path' or a 'url' pointing to a valid IG produced by HL7s publisher.")
            sys.exit(1)

        # These are plain zip files, so just open it up using python's Zipfile
        zipped = zipfile.ZipFile(f)

        # Iterate over each of the entries 
        for f in zipped.infolist():
            filename = f.filename

            # Make sure the file matches the expectations
            if filename.split("-")[0] in resource_list:
                if test_exclusion(filename, exclusion_list):
                    excluded_list.append(filename)
                else:
                    resources[filename] = json.loads(zipped.read(filename).decode())

        print(f"{len(resources)} resources found at: {ig_source}")

        # First, let's try deleting any that may already exist
        deleted_items = []
        for fn,data in resources.items():
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
                        pdb.set_trace()
                        print(len(resp))
        if len(deleted_items) > 0:
            print(f"Sleeping to give the backend time to catchup")

            sleep(args.sleep_time)

        # Iterate over the list and load them one at a time
        for fn,data in resources.items():
            response = fhir_client.load(data['resourceType'], data)
            if type(response) is dict:
                response = [response]
            
            for resp in response:
                if resp['status_code'] < 300:
                    print(f"Loading {fn} - {resp['status_code']}")
                else:
                    print(f"An error occurred loading {fn}")
                    print(resp['status_code'])
                    print(resp['issue'])
                    pdb.set_trace()


        print("Files Loaded: " + ", ".join(sorted(resources.keys())))
        print("Files Excluded: " + ", ".join(sorted(excluded_list)))

    print("""   
          \nPlease note that for some FHIR Servers, we've noticed that """ 
          """loading large vocabularies can take quite some time before the """
          """server is ready to use them and any changes made afterward. """)
