#!/usr/bin/env python

"""Executes the pipeline from start to finish"""

from pathlib import Path
from wstlr.conceptmap import BuildConceptMap
from wstlr.extractor import DataCsvToObject
from  subprocess import run
import json
from ncpi_fhir_client.fhir_client import FhirClient
from ncpi_fhir_client import fhir_auth
from yaml import safe_load
import sys
from argparse import ArgumentParser, FileType
import re
#from bs4 import BeautifulSoup
import requests
from wstlr.load import ResourceLoader
from wstlr.idcache import IdCache
from wstlr.bundle import Bundle, ParseBundle, RequestType

import pdb

# whistle -harmonize_code_dir_spec code_harmonization/ 
#       -input_file_spec output/whistle-input/phs001616.json 
#       -mapping_file_spec emerge.wstl 
#       -lib_dir_spec projector_library/ 
#       -verbose 
#       --output_dir output
def run_whistle(whistlefile, inputfile, harmonydir, projectorlib, outputdir):
    command = ['whistle', '-harmonize_code_dir_spec', harmonydir,
                    '-input_file_spec', inputfile,
                    '-mapping_file_spec', whistlefile,
                    '-lib_dir_spec', projectorlib, 
                    '-verbose',
                    '-output_dir', outputdir]
    result = run(command, capture_output=True)

    if result.returncode != 0:
        print(f"Std out    : {result.stdout.decode()}")
        print(f"Std Err    : {result.stderr.decode()}")
        print("\n🤦An error was encountered.🙉 Something was out of tune.")
        print(f"The command was {' '.join(command)}")
        sys.exit(1)
    else:
        final_result = f"{outputdir}/{str(inputfile).split('/')[-1].replace('.json', '.output.json')}"
        print(f"🎶 Beautifully played.🎵 \nResulting File: {final_result}")

    return f"{outputdir}/{Path(inputfile).stem}.output.json"

def get_latest_date(filename, latest_observed_date):
    mtime = Path(filename).stat().st_mtime

    if latest_observed_date is None or mtime > latest_observed_date:
        return mtime
    return latest_observed_date

def check_latest_update(config_filename, config):
    latest_update = get_latest_date(config_filename, None)

    for table in config['dataset']:
        if 'data_dictionary' in config['dataset'][table]:
            latest_update = get_latest_date(config['dataset'][table]['data_dictionary']['filename'], latest_update)
        latest_update = get_latest_date(config['dataset'][table]['filename'], latest_update)

    latest_update = get_latest_date(config['whistle_src'], latest_update)
    for wst in Path(config['projector_lib']).glob("*.wstl"):
        latest_update = get_latest_date(wst, latest_update)
    
    return latest_update

def example_config(writer, auth_type=None):
    """Returns a block of text containing one or all possible auth modules example configurations"""

    modules = fhir_auth.get_modules()
    print(
        f"""# Example Hosts Configuration.
# 
# This is a basic yaml file (yaml.org) where each root level tag represents a 
# system "name" and it's children's keys represent key/values to assign to a 
# host configuration which includes the authentication details.
#
# All host entries should have the following key/values:
# host_desc             - This is just a short description which can be used
#                         for log names or whatnot
# target_service_url    - This is the URL associated with the actual API 
# auth_type             - This is the module name for the authentication used
#                         by the specified host
#
# Please note that there can be multiple hosts that use the same authentication
# mechanism. Users must ensure that each host has a unique "key" """,
        file=writer,
    )
    for key in modules.keys():
        if auth_type is None or auth_type == key:
            other_entries = {
                "host_desc": f"Example {key}",
                "target_service_url": "https://example.fhir.server/R4/fhir",
            }

            modules[key].example_config(writer, other_entries)

def exec():
    host_config_filename = Path("fhir_hosts")

    if not host_config_filename.is_file() or host_config_filename.stat().st_size == 0:
        example_config(sys.stdout)
        sys.stderr.write(
            f"""
A valid host configuration file, fhir_hosts, must exist in cwd and was not 
found. Example configuration has been written to stout providing examples 
for each of the auth types currently supported.\n"""
        )
        sys.exit(1)

    host_config = safe_load(host_config_filename.open("rt"))
    # Just capture the available environments to let the user
    # make the selection at runtime
    env_options = sorted(host_config.keys())
    
    parser = ArgumentParser(
        description="Transform a DbGAP dataset table dictionary into a FHIR CodeSystem, then transform that into ConceptMaps."
    )
    parser.add_argument(
        "-e",
        "--env",
        choices=env_options,
        help=f"Remote configuration to be used to access the FHIR server. If no environment is provided, the system will stop after generating the whistle output (no validation, no loading)",
    )
    parser.add_argument(
        "-v",
        "--validate-only",
        action='store_true',
        help="Indicate that submissions to the FHIR server are just validation calls and not for proper loading. Anything that fails validation result in a termination."
    )
    parser.add_argument(
        "-m",
        "--max-validations",
        type=int,
        default=1000,
        help="If validating instead of loading, this determines how many of a given resource type will be validated. Values less than one means no limit to the number of resources validated."
    )
    parser.add_argument(
        "-b", 
        "--save-bundle", 
        action='store_true',
        help="Update the bundled output into a valid transaction bundle."
    )
    parser.add_argument(
        "-x",
        "--bundle-only",
        action='store_true',
        help="Bundles do require an environment, but with this set, nothing will be submitted to the actual fhir server (sets the 'save-bundle' flag)"
    )
    parser.add_argument(
        "-i",
        "--intermediate",
        default='output/whistle-input',
        help="Path for intermediate data components"
    )
    parser.add_argument(
        "-f", 
        "--force",
        action='store_true',
        help="Run Whistle even if none of the input files (or projector files) have changed"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="output/whistle-output",
        help="Local output from whistle"
    )
    parser.add_argument(
        "config",
        nargs='+',
        type=FileType('rt'),
        help="Dataset YAML file with details required to run conversion.",
    )
    args = parser.parse_args(sys.argv[1:])

    if args.bundle_only:
        args.save_bundle = True

    for config_file in args.config:
        config = safe_load(config_file)

        # Build ConceptMaps if provided
        for dataset in config['dataset'].keys():
            if 'code_harmonization' in config['dataset'][dataset]:
                BuildConceptMap(config['dataset'][dataset]['code_harmonization'])

        # Work out the destination for the Whistle input
        output_directory = Path(args.intermediate)
        output_directory.mkdir(parents=True, exist_ok=True)
        whistle_input = output_directory / f"{config['output_filename']}.json"

        dataset = DataCsvToObject(config)

        input_file_ts = check_latest_update(config_file.name, config)

        if args.force or not whistle_input.exists() or input_file_ts > whistle_input.stat().st_mtime:
            with whistle_input.open(mode='wt') as f:
                f.write(json.dumps(dataset, indent=2))

        output_directory = Path(args.output)
        output_directory.mkdir(parents=True, exist_ok=True)
        whistle_output = output_directory / f"{config['output_filename']}.output.json"

        if args.force or not whistle_output.exists() or whistle_output.stat().st_mtime < input_file_ts:
            result_file = run_whistle(whistlefile=config['whistle_src'], 
                        inputfile=str(whistle_input), 
                        harmonydir=config['code_harmonization_dir'], 
                        projectorlib=config['projector_lib'], 
                        outputdir=str(output_directory))
        else:
            result_file = str(whistle_output)
            print(f"Skipping whistle since none of the input has changed")

        if args.env:
            if args.max_validations > 0:
                ResourceLoader._max_validations_per_resource = args.max_validations
            fhir_client = FhirClient(host_config[args.env])
            cache = IdCache(config['study_id'], fhir_client.target_service_url)
            loader = ResourceLoader(config['identifier_prefix'], fhir_client, idcache=cache)

            resource_consumers = []

            # if we are loading, we'll grab the loader so that we can 
            if args.validate_only:
                resource_consumers.append(loader.consume_validate)
            elif not args.bundle_only:
                resource_consumers.append(loader.consume_load)

            transaction_bundle = None
            if args.save_bundle:
                bundle_filename = output_directory / f"{Path(result_file).stem}-transaction.json"
                request_type = RequestType.PUT
                if args.validate_only or args.bundle_only:
                    request_type = RequestType.POST
                transaction_bundle = Bundle(bundle_filename, f"{config['study_id']}-bundle", fhir_client.target_service_url, request_type=request_type)
                resource_consumers.append(transaction_bundle.consume_resource)

            with open(result_file, 'rt') as  f:
                ParseBundle(f, resource_consumers)

            if not args.validate_only:
                while len(loader.delayed_loading) > 0:
                    print(f"Attempting to load {len(loader.delayed_loading)} left-overs. ")
                    loader.retry_loading()

            if args.save_bundle:
                transaction_bundle.close_bundle()
