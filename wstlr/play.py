#!/usr/bin/env python

"""Executes the pipeline from start to finish"""

from pathlib import Path
from wstlr.conceptmap import BuildConceptMap
from wstlr.extractor import DataCsvToObject
from wstlr.inspector import ResourceInspector, ObservationInspector
from wstlr.module_summary import ModuleSummary

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
from wstlr import get_host_config
from wstlr.load import ResourceLoader
from wstlr.idcache import IdCache
from wstlr.bundle import Bundle, ParseBundle, RequestType

from ncpi_fhir_client.ridcache import RIdCache
from wstlr.config import Configuration

import os

import pdb

# whistle -harmonize_code_dir_spec code_harmonization/ 
#       -input_file_spec output/whistle-input/phs001616.json 
#       -mapping_file_spec emerge.wstl 
#       -lib_dir_spec projector_library/ 
#       -verbose 
#       --output_dir output
def run_whistle(whistlefile, inputfile, harmonydir, projectorlib, outputdir, whistle_path='whistle'):
    command = [whistle_path, '-harmonize_code_dir_spec', harmonydir,
                    '-input_file_spec', inputfile,
                    '-mapping_file_spec', whistlefile,
                    '-lib_dir_spec', projectorlib, 
                    '-verbose',
                    '-output_dir', outputdir]
    #print(" ".join(command))
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
    if filename is None or str(filename).lower() == 'none':
        return latest_observed_date

    mtime = Path(filename).stat().st_mtime

    if latest_observed_date is None or mtime > latest_observed_date:
        return mtime
    return latest_observed_date

def check_latest_update(config, cm_timestamp = None):
    latest_update = get_latest_date(config.filename, None)

    # Work the harmony concept map into dependency check
    if cm_timestamp is not None and latest_update < cm_timestamp:
        latest_update = cm_timestamp

    for table_name, table in config.dataset.items():
        if 'data_dictionary' in table:
            latest_update = get_latest_date(table['data_dictionary']['filename'], latest_update)

        for filename in table['filename'].split(","):
            latest_update = get_latest_date(filename, latest_update)

    latest_update = get_latest_date(config.whistle_src, latest_update)
    for wst in Path(config.projector_lib).glob("*.wstl"):
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
    host_config = get_host_config()
    # Just capture the available environments to let the user
    # make the selection at runtime
    env_options = sorted(host_config.keys())
    
    parser = ArgumentParser(
        description="Transform a DbGAP dataset table dictionary into a FHIR CodeSystem, then transform that into ConceptMaps."
    )
    parser.add_argument(
        "--host",
        choices=env_options,
        default=None,
        help=f"Remote configuration to be used to access the FHIR server. If no environment is provided, the system will stop after generating the whistle output (no validation, no loading)",
    )
    parser.add_argument(
        "-e", 
        "--env", 
        choices=["local", "dev", "qa", "prod"],
        help=f"If your config has host details configured, you can use these short cuts to choose the appropriate host details. This is useful if you wish to run different configurations on the same command, but each has a different target host. "
    )
    parser.add_argument(
        "-v",
        "--validate-only",
        action='store_true',
        help="Indicate that submissions to the FHIR server are just validation calls and not for proper loading. Anything that fails validation result in a termination."
    )
    parser.add_argument(
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
    parser.add_argument(
        "--thread-count",
        type=int,
        default=10,
        help="Number of threads to use when using threaded loads"
    )

    parser.add_argument(
        "-t",
        "--threaded", 
        action='store_true',
        help="When true, loads will be submitted in parallel."
    )
    parser.add_argument(
        "-lb",
        "--load-buffer-size",
        default=5000,
        type=int,
        help="Number of records to buffer before launching threaded loads. Only matters when running with async=true"
    )
    parser.add_argument(
        "-m",
        "--module",
        type=str,
        action='append',
        help="When loading resources into FHIR, this indicates the name of a module to be loaded. A module is a 'root' level entry in the whistle output object. --module may be specified multiple times to load multiple modules."
    )
    parser.add_argument(
        "-r", 
        "--resource",
        type=str,
        action='append',
        help="When loading resources into FHIR, this indicates a resourceType that will be loaded. --resource may be specified more than once."
    )
    args = parser.parse_args(sys.argv[1:])

    if args.bundle_only:
        args.save_bundle = True

    host = args.host

    for config_file in args.config:
        cfg = Configuration(config_file)
        #config = safe_load(config_file)
        require_official = cfg.require_official

        environment = cfg.env
        if args.env is not None:
            if args.env not in environment:
                print(f"The environment, {args.env}, is not configured in {cfg.filename}.")
                sys.exit(1)

            if args.host is not None:
                print(f"Specifying both a host and and environment doesn't make sense. Please use only --env or --host")
                sys.exit(1)

            host = environment[args.env]
        # Work out the destination for the Whistle input
        output_directory = Path(args.intermediate)
        output_directory.mkdir(parents=True, exist_ok=True)
        whistle_input = output_directory / f"{cfg.output_filename}.json"

        try:
            dataset = DataCsvToObject(cfg)
        except FileNotFoundError as e:
            sys.stderr.write(f"ERROR: Unable to find file, {e.filename}.\n")
            sys.exit(1)

        harmony_files = set()
        cm_timestamp = None
        # Build ConceptMaps if provided
        for dsname, dsconfig in cfg.dataset.items():
            if 'code_harmonization' in dsconfig and \
                dsconfig['code_harmonization'] not in harmony_files:
                # We do want to rebuild each harmony file once per config, but
                # no need to do it more than that
                cm_timestamp = BuildConceptMap(
                            dsconfig['code_harmonization'], 
                            curies=cfg.curies, 
                            codesystems=dataset['code-systems'])
                harmony_files.add(dsconfig['code_harmonization'])

        input_file_ts = check_latest_update(cfg, cm_timestamp)

        if args.force or not whistle_input.exists() or input_file_ts > whistle_input.stat().st_mtime:
            with whistle_input.open(mode='wt') as f:
                f.write(json.dumps(dataset, indent=2))

        output_directory = Path(args.output)
        output_directory.mkdir(parents=True, exist_ok=True)
        whistle_output = output_directory / f"{cfg.output_filename}.output.json"

        if args.force or not whistle_output.exists() or whistle_output.stat().st_mtime < input_file_ts:
            response = run(['which', 'whistle'], capture_output=True)
            whistle_path = 'whistle'
            if response.returncode != 0:
                print("Unable to find whistle in the PATH")
                print("PATH: " + "\n\t".join(os.getenv("PATH").split(":")))
                sys.exit()
            else: 
                whistle_path = response.stdout.decode().strip()
                print(f"Whistle Path: {whistle_path}")

            result_file = run_whistle(whistlefile=cfg.whistle_src, 
                        inputfile=str(whistle_input), 
                        harmonydir=cfg.code_harmonization_dir, 
                        projectorlib=cfg.projector_lib, 
                        outputdir=str(output_directory),
                        whistle_path=whistle_path)

            # We really only want to run this when we generate a new Whistle file,
            # so we'll do this work separately from the other consumers
            resource_inspector = ResourceInspector(require_official=require_official)
            obs_inspector = ObservationInspector()
            resource_summary = ModuleSummary()
            with open(result_file, 'rt') as  f:
                ParseBundle(f, [resource_inspector.check_identifier, 
                                obs_inspector.inspect, 
                                resource_summary.summary])            
            resource_summary.print_summary()
        else:
            result_file = str(whistle_output)
            print(f"Skipping whistle since none of the input has changed")

        if host:  
            if args.max_validations > 0:
                ResourceLoader._max_validations_per_resource = args.max_validations
            cache_remote_ids = RIdCache(study_id=cfg.study_id, 
                                        valid_patterns=cfg.fhir_id_patterns)
            fhir_client = FhirClient(host_config[host], 
                                        idcache=cache_remote_ids)

            #cache = IdCache(config['study_id'], fhir_client.target_service_url)
            loader = ResourceLoader(cfg.identifier_prefix, 
                                        fhir_client, 
                                        study_id=cfg.study_id, 
                                        resource_list=args.resource, 
                                        module_list=args.module, 
                                        idcache=cache_remote_ids, 
                                        threaded=args.threaded, 
                                        thread_count=args.thread_count)
            if args.threaded:
                print("Threading enabled")
                loader.max_queue_size = args.load_buffer_size
            resource_consumers = []

            # if we are loading, we'll grab the loader so that we can 
            if args.validate_only:
                resource_consumers.append(loader.consume_validate)
            elif not args.bundle_only:
                resource_consumers.append(loader.consume_load)

            transaction_bundle = None
            if args.save_bundle:
                bundle_filename = output_directory / f"{Path(result_file).stem.replace('.output', '')}"
                request_type = RequestType.PUT
                if args.validate_only or args.bundle_only:
                    request_type = RequestType.POST
                transaction_bundle = Bundle(bundle_filename, 
                                            f"{cfg.study_id}-bundle", 
                                            fhir_client.target_service_url, 
                                            request_type=request_type)
                resource_consumers.append(transaction_bundle.consume_resource)

            with open(result_file, 'rt') as  f:
                ParseBundle(f, resource_consumers)
            
            max_final_attempts = 10
            if not args.validate_only:
                while len(loader.delayed_loading) > 0 and max_final_attempts > 0:
                    # Make sure we clear out the queue in case there are some 
                    # things there that these reloads depend on
                    #pdb.set_trace()
                    loader.launch_threads()

                    print(f"Attempting to load {len(loader.delayed_loading)} left-overs. ")
                    loader.retry_loading()
                    max_final_attempts -= 1

            # Launch anything that was lingering in the queue
            loader.cleanup_threads()
            loader.print_summary()
            loader.save_fails(output_directory / f"invalid-references.json")
            loader.save_study_ids(output_directory / f"study-ids.json")

            if args.save_bundle:
                transaction_bundle.close_bundle()
