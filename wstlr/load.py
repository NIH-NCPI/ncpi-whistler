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

from wstlr.studyids import StudyIDs

import concurrent.futures
from threading import Lock, current_thread, main_thread
import datetime

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
        if key == "identifier" and parent_key is not None:
            assert(type(value) is dict)
            if 'value' not in value:
                pdb.set_trace()
            idcomponents = idcache.get_id(value['system'], value['value'])
            if idcomponents is not None:
                resource_type, id = idcomponents

                del record[key]
                record['reference'] = f"{resource_type}/{id}"
            else:
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
    def __init__(self, identifier_prefix, fhir_client, study_id, idcache=None, threaded=False):
        self.identifier_prefix = identifier_prefix
        self.identifier_rx = re.compile(identifier_prefix)
        self.client = fhir_client
        self.idcache = idcache

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

        self.records_loaded = 0
        if threaded:
            self.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=32)

    def get_identifier(self, resource):
        if 'identifier' in resource:
            for identifier in resource['identifier']:
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

    def save_study_ids(self, filename):
        self.studyids.dump_to_file(filename)
    
    def cleanup_threads(self):
        if self.thread_executor is not None:
            self.launch_threads()

            self.thread_executor.shutdown(wait=True)

    def add_job_to_queue(self, resource):
        # Run immediately if there is no executor or if it's one of the ontontology types
        if resource['resourceType'] not in ['CodeSystem', 'ValueSet'] or self.thread_executor is not None:
            self.load_queue.append(self.thread_executor.submit(self.load_resource, resource))

            if self.max_queue_size <= len(self.load_queue):
                self.launch_threads()
        else:
            self.load_resource(resource)


    def consume_load(self, group_name, resource):
        try:
            #pdb.set_trace()

            with load_lock:
                build_references(resource, self.idcache, parent_key=None)
            
            self.add_job_to_queue(resource)

        except InvalidReference as e:
            self.delayed_loading.append(resource)

    def retry_loading(self, resources=None):
        if resources is None:
            resources = self.delayed_loading

        delayed_again = []
        for resource in resources:
            try:
                with load_lock:
                    build_references(resource, self.idcache, parent_key=None)
                self.add_job_to_queue(resource)

            except InvalidReference as e:
                with load_lock:
                    print(e.message())
                    delayed_again.append(resource)
                
        with load_lock:
            self.delayed_loading = delayed_again

    def consume_validate(self, group_name, resource):
        """Do we even care to use async with validate? I'm skipping it for now"""
        self.load_resource(resource, validate_only=True)

    def load_resource(self, resource, validate_only=False):
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
        if resource_type in ['CodeSystem', 'ValueSet']:
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
            result = self.client.post(resource_type, 
                                        resource, 
                                        identifier=resource_identifier,
                                        identifier_system=system,
                                        identifier_type=identifier_type,
                                        validate_only=validate_only)
        if result['status_code'] < 300:
            #pdb.set_trace()
            self.studyids.add_id(resource_type, result['response']['id'])

            if cache_id and 'id' in result['response']:
                self.idcache.store_id(resource_type, system, uniqid, result['response']['id'], no_db=True)

        else:
            skipped_warnings = 0
            skipped_errors = 0
            error_count = 0
            last_error = None
            for issue in result['response']['issue']:
                if issue['severity'] == 'error':
                    if error_count < 5:
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



        





