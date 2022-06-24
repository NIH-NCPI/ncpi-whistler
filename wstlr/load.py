"""Load output from whistle into a FHIR server

to facilitate overwriting pre-existing records, a unique identifier
must be present under a system with a common prefix. This requires
the IdCache object which performs the actual caching. 

"""

import re
import sys
from collections import defaultdict
from pprint import pformat
import pdb
from pathlib import Path
from argparse import ArgumentParser, FileType
import json
from copy import deepcopy
from time import sleep

from pathlib import Path
from ncpi_fhir_client.fhir_client import FhirClient
from yaml import safe_load
from wstlr.studyids import StudyIDs

import concurrent.futures
from threading import Lock, current_thread, main_thread
import datetime

from wstlr.bundle import Bundle, ParseBundle, RequestType

from ncpi_fhir_client.ridcache import RIdCache


load_lock = Lock()  # Lock used for basic load functionality like ID fetching 
                    # and updating

id_lock = Lock()    # Lock used during insertion into the observed IDs 
                    # This is a different resource than the load_lock
class InvalidReference(Exception):
    def __init__(self, identifier, key):
        self.system = identifier['system']
        self.value = identifier['value']
        self.key = key
        #pdb.set_trace()
        super().__init__(self.message())

    def message(self):
        return f"Unseen reference to {self.key}=>{self.system}:{self.value}"

def build_references(record, idcache, parent_key=None):
    """Replace the identifier references with identifiers with actual IDs from 
    successful inserts. Please note the ID that will be matched on will be
    the first identifier, so please make sure the data is correctly defined."""

    # Bulk Export doesn't order things as nicely as it could
    # so we may need to 
    for key, value in record.items():
        # Containers are backbone items, which will probably have an identifier that
        # doesn't work like a reference
        if key == "identifier" and parent_key is not None and parent_key != 'container':
            if type(value) is not dict:
                pdb.set_trace()
            assert(type(value) is dict)
            if 'value' not in value:
                pdb.set_trace()
            idcomponents = idcache.get_id(value['system'], value['value'])
            if idcomponents is not None:
                resource_type, id = idcomponents

                del record[key]
                record['reference'] = f"{resource_type}/{id}"
            else:
                #print(value)
                #pdb.set_trace()
                raise InvalidReference(value, parent_key)
        else:
            if type(value) is list:
                for item in value:
                    if type(item) is dict:
                        build_references(item, idcache, parent_key=key)

            if type(value) is dict:
                build_references(value, idcache, parent_key=key)

            


# This is the prefix that will be used to identify the resource if it
# possibly exists already inside the target FHIR server. This MUST be
# present in it's entirety at the start of an identifier's system string
class ResourceLoader:
    # values less than 1 will mean validate all resources
    # This should do nothing if records are actually being loaded and 
    # not validated
    _max_validations_per_resource = -1

    _resource_buffer = []
    def __init__(self, identifier_prefix, fhir_client, study_id, resource_list=None, module_list=None, idcache=None, threaded=False, thread_count=10):
        self.identifier_prefix = identifier_prefix
        self.identifier_rx = re.compile(identifier_prefix)
        self.client = fhir_client
        self.idcache = idcache

        if module_list is None:
            module_list = []
        if resource_list is None:
            resource_list = []
        self.module_list = set(module_list)
        self.resource_list = set(resource_list)

        self.study_id = study_id

        self.studyids = StudyIDs(fhir_client.target_service_url, study_id)
        # We'll write this out at the end of the run so that we can have a complete
        # list of IDs for purging if we want to clean them out
        self.observed_record_ids = defaultdict(list)

        # resourceType => # seen Useful only for validation cutoff.
        self.resources_observed = defaultdict(int)

        # For resources which reference something we haven't loaded
        # yet, we'll stash them here and retry them when the bundle is done
        self.delayed_loading = []

        # Load Buffer size
        # We don't want to add an infinite number of records to the queue
        # in case it causes memory issues, so we'll block loading new
        # records until the futures are completed then we start building
        # our queue back up
        self.max_queue_size = 500
        self.load_queue = []
        self.thread_executor = None

        self.successful_loads = defaultdict(lambda: defaultdict(int))
        self.resource_summary = defaultdict(int)

        self.records_loaded = 0
        if threaded:
            self.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_count)

    def get_identifier(self, resource):
        if 'identifier' in resource:
            identifiers = deepcopy(resource['identifier'])

            if type(identifiers) is not list:
                identifiers = [identifiers]

            for identifier in identifiers:
                if 'system' in identifier:
                    with load_lock:
                        id_match = self.identifier_rx.match(identifier['system'])
                    
                    if id_match:
                        if 'value' not in identifier:
                            pdb.set_trace()
                        return (identifier['system'], identifier['value'])
        return None

    def launch_threads(self):
        """This should be called before the application exits

        There is no harm in calling it even during a non-asynchronous run
        """
        if self.thread_executor is not None:
            start_time = datetime.datetime.now()
            self.records_loaded += len(self.load_queue)
            print(f"Launching threads ({len(self.load_queue)} | {self.records_loaded})")
            for entry in concurrent.futures.as_completed(self.load_queue):
                entry.result()
            print(f"Thread queue ({len(self.load_queue)}) completed in {(datetime.datetime.now() - start_time).seconds}s")
            self.load_queue = []

    def save_fails(self, filename):
        data = {}
        savefile = Path(filename)

        if savefile.exists():
            with savefile.open('rt') as f:
                data = json.load(f)
        
        if self.study_id not in data:
            data[self.study_id] = {
                self.client.target_service_url: {
                    "bad_references": []
                }
            }
        
        if self.client.target_service_url not in data[self.study_id]:
            data[self.study_id][self.client.target_service_url] = {
                "bad_references": []
            }
        
        problems = []
        for group_name, resource in self.delayed_loading:
            try:
                build_references(resource, self.idcache)
            except InvalidReference as e:
                problems.append({
                    "error": e.message(),
                    "resource": resource
                })

        data[self.study_id][self.client.target_service_url]['bad_references'] = problems

        with savefile.open('wt') as f:
            json.dump(data, f, indent=2)
        print(f"{len(self.delayed_loading)} unloaded resources written to {filename}")

    def save_study_ids(self, filename):
        self.studyids.dump_to_file(filename)
    
    def cleanup_threads(self):
        if self.thread_executor is not None:
            self.launch_threads()

            self.thread_executor.shutdown(wait=True)

    def add_job_to_queue(self, group_name, resource):
        # Run immediately if there is no executor or if it's one of the ontontology types
        if resource['resourceType'] not in ['CodeSystem', 'ValueSet'] and self.thread_executor is not None:
            self.load_queue.append(self.thread_executor.submit(self.load_resource, group_name, resource))

            if self.max_queue_size <= len(self.load_queue):
                self.launch_threads()
        else:
            self.load_resource(group_name, resource)


    def consume_load(self, group_name, resource):
        if len(self.module_list) == 0 or group_name in self.module_list:
            if len(self.resource_list) == 0 or resource['resourceType'] in self.resource_list:
                try:

                    with load_lock:
                        if 'resourceType' not in resource:
                            print(pformat(resource))

                        build_references(resource, self.idcache, parent_key=None)

                    self.add_job_to_queue(group_name, resource)

                except InvalidReference as e:
                    self.delayed_loading.append((group_name, resource))

    def retry_loading(self, resources=None):
        if resources is None:
            resources = self.delayed_loading

        delayed_again = []
        for group_name, resource in resources:
            try:
                with load_lock:
                    if resource['resourceType'] == 'ObservationDefinition':
                        #pdb.set_trace()
                        pass
                    build_references(resource, self.idcache, parent_key=None)
                self.add_job_to_queue(group_name, resource)

            except InvalidReference as e:
                with load_lock:
                    #print(e.message())
                    delayed_again.append((group_name, resource))
                
        with load_lock:
            self.delayed_loading = delayed_again

    def consume_validate(self, group_name, resource):
        """Do we even care to use async with validate? I'm skipping it for now"""
        self.load_resource(group_name, resource, validate_only=True)

    def load_resource(self, group_name, resource, validate_only=False):
        cache_id = False
        resource_identifier = None
        system = None
        uniqid = None

        if 'resourceType' not in resource:
            print(pformat(resource))
            print("Ooops! There is a problem with this record!. No resourceType!")
            sys.exit(1)
        resource_type = resource['resourceType']

        resource_index = 0
        with load_lock:
            self.resources_observed[resource_type] += 1
            resource_index = self.resources_observed[resource_type]

        if current_thread() is not main_thread():
            current_thread().name = f"{resource_type}|{resource_index}"

        if validate_only and ResourceLoader._max_validations_per_resource > 0 and  self.resources_observed[resource_type] > ResourceLoader._max_validations_per_resource:
            # For now, we'll just return a successful status code
            return {"status_code" : 200 }
        # We'll handle CodeSystems and ValueSets differently
        if resource_type in ['CodeSystem', 'ValueSet', 'ConceptMap']:
            result = self.client.load(resource_type, resource, validate_only)
            if result['status_code'] < 300:
                (system, uniqid) =  self.get_identifier(result['response'])
                cache_id = True
        else:
            if self.idcache and 'id' not in resource:
                (system, uniqid) = self.get_identifier(resource)
                id = self.idcache.get_id(system, uniqid)

                if id:
                    resource['id'] = id[1]
                else:
                    cache_id = True

                identifier_type = "identifier"
                if resource_type in ['ObservationDefinition']:
                    #resource_identifier = f"{system}|{uniqid}"
                    resource_identifier = None
                else:
                    resource_identifier = uniqid
                """
                else:
                    # ObservationDefinition is very early stages and has 
                    # no real search properties defined. So, we can't
                    # recall a prexisting ID if it isn't in our DB
                    resource_identifier = None
                    """
                """
                    code_value = resource['code']['coding'][0]
                    code_system = code_value['system']
                    code = code_value['code']
                    resource_identifier = f"{code_system}:{code}"
                    identifier_type = "code"
                    """

            # If we couldn't find an id in the cache, we will pass the
            # identifier parameter in so that it will attempt to "get" 
            # a match and reuse the ID for a valid PUT overwrite
            retry_count = FhirClient.retry_post_count
            while retry_count > 0:
                retry_count -= 1
                result = self.client.post(resource_type, 
                                            resource, 
                                            identifier=resource_identifier,
                                            identifier_system=system,
                                            identifier_type=identifier_type,
                                            validate_only=validate_only)
                if result['status_code'] < 300:
                    retry_count = 0

                elif result['status_code'] == 429:
                    print(f"\t492 : {resource_type}:{resource_identifier}")
                    if retry_count == 0:
                        print("\tNot actually going to try again. So..ugh")
                        pdb.set_trace()
                    sleep(35)
                else:
                    print(f"\t{result['status_code']} : {result['request_url']}")
                    sleep(5)
        if result['status_code'] < 300:
            self.successful_loads[group_name][resource_type] += 1
            self.resource_summary[resource_type] += 1
            #pdb.set_trace()
            self.studyids.add_id(resource_type, result['response']['id'])

            if cache_id and 'id' in result['response']:
                self.idcache.store_id(resource_type, system, uniqid, result['response']['id'], no_db=True)

        else:
            skipped_warnings = 0
            skipped_errors = 0
            error_count = 0
            last_error = None
            if 'issue' not in result['response']:
                print(pformat(result['response']))
                pdb.set_trace()
            for issue in result['response']['issue']:
                if issue['severity'] == 'error':
                    if error_count < 5:
                        print(pformat(resource))
                        print(pformat(issue, width=160, compact=True))
                        error_count += 1
                    else:
                        last_error = issue
                        skipped_errors += 1
                else:
                    skipped_warnings += 1 

            if last_error is not None:
                with load_lock:
                    print(pformat(last_error, width=160, compact=True))
                    skipped_errors -= 1
            with load_lock:
                print(f"Skipped {skipped_warnings} warnings and {skipped_errors}.")

                pdb.set_trace()
            sys.exit(1)
        return result


    def print_summary(self):
        print("Load Summary\n")
        print("Module Name                      Resource Type            #         % of Total")
        print("-------------------------------  ------------------------ --------- ----------")
        for modulename in sorted(self.successful_loads.keys()):
            for resourcetype in sorted(self.successful_loads[modulename].keys()):
                observed = self.successful_loads[modulename][resourcetype]
                total = self.resource_summary[resourcetype]
                perc = f"{(100.0 * observed)/total:4.2f}" 
                print(f"{modulename:<32} {resourcetype:<24} {self.successful_loads[modulename][resourcetype]:<9} {perc:>7}")


def exec():
    host_config_filename = Path("fhir_hosts")

    host_config = safe_load(host_config_filename.open("rt"))
    # Just capture the available environments to let the user
    # make the selection at runtime
    env_options = sorted(host_config.keys())
    
    parser = ArgumentParser(
        description="Load whistle output file into selected FHIR server."
    )
    pdb.set_trace()
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
        "-mv",
        "--max-validations",
        type=int,
        default=1000,
        help="If validating instead of loading, this determines how many of a given resource type will be validated. Values less than one means no limit to the number of resources validated."
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
        "--require-official",
        type=bool,
        default=True
    )    
    parser.add_argument(
        "-s",
        "--study-id",
        required=True,
        type=str,
        help="Study ID which will be found in the meta.tag (bad things will happen if this is wrong)"
    )
    parser.add_argument(
        "--fhir-id-patterns",
        type=str,
        default=None,
        help="I need some time to figure what this did..."
    )
    parser.add_argument(
        "--identifier-prefix",
        type=str,
        required=True,
        help="This is used throughout whistle. It's probably necessary for bundle generation, which shouldn't matter here, but go ahead and provide it for now..."
    )
    parser.add_argument(
        "file",
        required=True,
        type=FileType('rt'),
        help="JSON output from Whistle to be inspected.",
    )
    args = parser.parse_args(sys.argv[1:])

    if args.max_validations > 0:
        ResourceLoader._max_validations_per_resource = args.max_validations
    cache_remote_ids = RIdCache(study_id=args.study_id, valid_patterns=args.fhir_id_patterns)
    fhir_client = FhirClient(host_config[args.env], idcache=cache_remote_ids)

    #cache = IdCache(config['study_id'], fhir_client.target_service_url)
    print(args)
    pdb.set_trace()
    loader = ResourceLoader(args.identifier_prefix, fhir_client, resource_list=args.resource, module_list=args.module, study_id=args.study_id, idcache=cache_remote_ids, threaded=args.threaded)

    if args.threaded:
        print("Threading enabled")
        loader.max_queue_size = args.load_buffer_size
    resource_consumers = []

    # if we are loading, we'll grab the loader so that we can 
    if args.validate_only:
        resource_consumers.append(loader.consume_validate)
    else:
        resource_consumers.append(loader.consume_load)

    with open(args.file, 'rt') as  f:
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
    output_directory = Path(args.file.name).parent
    loader.save_fails(output_directory / f"invalid-references.json")
    loader.save_study_ids(output_directory / f"study-ids.json")



        





