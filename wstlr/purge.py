#!/usr/bin/env python

from wstlr.studyids import StudyIDs
from wstlr.hostfile import load_hosts_file
from collections import defaultdict

from ncpi_fhir_client.fhir_client import FhirClient
from ncpi_fhir_client import default_resources
from copy import deepcopy

import datetime
import time
import concurrent.futures
from threading import Lock, current_thread, main_thread
import pdb

import sys
from argparse import ArgumentParser, FileType

del_lock = Lock()

resource_order = [
    'CodeSystem',
    'ValueSet',
    'ObservationDefinition', 
    'ActivityDefinition',
    'Organization',
    'Patient',
    'Group',
    'Specimen',
    'Substance',
    'Encounter',
    'Observation',
    'Condition',
    'ResearchStudy',
    'ResearchSubject',
    'DocumentReference',
    'Task'
]

class ResourceDeleter:
    def __init__(self, client, threaded=False, max_queue_size=5000, thread_count=10):
        self.client = client
        self.studyids = None

        self.threaded = threaded
        self.thread_count = thread_count

        # resource type => [id1, id2, id3]
        self.ids_to_delete = defaultdict(list)
        self.delayed_deletes = defaultdict(list)
        self.records_purged = 0
        self.threaded = threaded
        self.max_queue_size = max_queue_size
        self.del_queue = []
        self.thread_executor = None        
        
        if threaded:
            self.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_count)

    def load_studyids(self, filename):
        self.studyids = StudyIDs(self.client.target_service_url)
        return self.studyids.load_from_file(filename)

    def delete_resources_by_tag(self, study_id, resource_list=None):
        if resource_list is None or 'ALL' in resource_list:
            resource_list = default_resources(self.client)

        # First, delete anything that isn't in the order...assuming those are less likely to 
        # have dependant resources point back to them:

        ordered_resources = default_resources(self.client, ignore_resources=resource_order + ['Bundle']) +  resource_order[::-1]

        for resource in ordered_resources:
            if resource in resource_list or 'ALL' in resource_list:
                qry = f"{resource}?_tag={study_id}&_elements=id"

                response = self.client.get(qry)    
                ids = []
                if len(response.entries) > 0:
                    print(f"Purging All IDS from {study_id}:{resource}")

                for entry in response.entries:
                    # If it's an empty bundle, then there won't be a resource
                    if 'resource' in entry:
                        self.add_job_to_queue(resource, entry['resource']['id'])
                self.launch_threads()

                if response.response['total'] > 0:
                    print(f"{resource} : {response.response['total']}")            

    def delete_resources(self, study_id, resource_list=None):
        global resource_order

        if resource_list is None or resource_list == ['ALL']:
            if self.studyids is None:
                print("You must first provide a studyids file to purge by resource")
                sys.exit(1)

            resource_list = self.studyids.list_resource_types(study_id)

        ordered_resources = []
        for resource in resource_list:
            if resource not in resource_order:
                ordered_resources.append(resource)

        ordered_resources += resource_order[::-1]

        for resource in ordered_resources:
            if resource in resource_list:
                ids = self.studyids.get_ids(study_id, resource)[::-1]
                print(f"Deleting {len(ids)} from {resource}")
                #pdb.set_trace()
                self.ids_to_delete[resource] += ids

                for id in self.ids_to_delete[resource]:
                    self.add_job_to_queue(resource, id)
                
                self.launch_threads()

    def cleanup_threads(self):
        if self.thread_executor is not None:
            self.launch_threads()

            self.thread_executor.shutdown(wait=True)

    def retry_purge(self):
        print("Retrying conflicted resources")
        for i in range(0, 5):
            # Give the database some time to catch up
            print(f"#{i} - Sleeping a bit before retrying the deletions")
            time.sleep(60)
            if len(self.delayed_deletes) > 0:
                self.ids_to_delete = self.delayed_deletes
                self.delayed_deletes = defaultdict(list)
                ordered_resources = []
                ordered_resources = default_resources(self.client, ignore_resources=resource_order + ['Bundle'])

                ordered_resources += resource_order[::-1]

                for resource in ordered_resources:
                    if resource in self.ids_to_delete:
                        for id in self.ids_to_delete[resource]:
                            self.add_job_to_queue(resource, id)

                self.launch_threads()
            else: 
                return

        if len(self.delayed_deletes) > 0:
            print("Resources have IDs that couldn't be deleted:")
            for resource in self.delayed_deletes.keys():
                print(f"\t{resource} - {len(self.delayed_deletes[resource])}")

    def launch_threads(self):
        if self.thread_executor is not None:
            start_time = datetime.datetime.now()
            self.records_purged += len(self.del_queue)
            print(f"Launching threads ({len(self.del_queue)} | {self.records_purged})")
            for entry in concurrent.futures.as_completed(self.del_queue):
                entry.result()
            print(f"Thread queue ({len(self.del_queue)}) completed in {(datetime.datetime.now() - start_time).seconds}s")
            self.del_queue = []


    def add_job_to_queue(self, resource, id):
        if self.thread_executor is not None:
            self.del_queue.append(self.thread_executor.submit(self.delete_resource, resource, id))

            if self.max_queue_size <= len(self.del_queue):
                self.launch_threads()
        else:
            self.delete_resource(resource, id)

    def delete_resource(self, resource, id):
        if current_thread() is not main_thread():
            current_thread().name = f"{resource}/{id}"

        if id == "207020":
            pdb.set_trace()
        response = self.client.delete_by_record_id(resource, id, silence_warnings=True)
        #pdb.set_trace()
        status_code = response['status_code']
        if status_code == 200:
            return
        elif status_code == 409:
            print(response['response']['issue'][0]['diagnostics'])
            #pdb.set_trace()
            self.delayed_deletes[resource].append(id)
        else:
            print(response)
            # self.delayed_deletes[resource].add(id)
            pdb.set_trace()
       

def exec(args=None):
    if args is None:
        args = sys.argv[1:]

    host_config = load_hosts_file("fhir_hosts")
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
        required=True,
        help=f"Remote configuration to be used to access the FHIR server. If no environment is provided, the system will stop after generating the whistle output (no validation, no loading)",
    )
    parser.add_argument(
        "-s", 
        "--study-ids",
        type=FileType('rt'),
        #default=open("output/whistle-output/study-ids.json", 'rt'),
        help="Name of the study IDs JSON file to pull IDs from"
    )
    parser.add_argument(
        "-tag",
        "--delete-files-by-tag",
        action='store_true',
        help="Rather than rely on the IDs from the study-ids file, simple delete based on the meta.tag"
    )
    parser.add_argument(
        "-t",
        "--threaded", 
        action='store_true',
        help="When true, deletes will be performed in parallel."
    )
    parser.add_argument(
        "-r",
        "--resource",
        choices=["ALL"] + resource_order,
        action='append',
        help="Add resource type to list of resources to purge"
    )
    parser.add_argument(
        "-n",
        "--study-name",
        required=False,
        type=str,
        help="Study ID to extract data from"
    )
    parser.add_argument(
        "-c",
        "--thread-count",
        type=int,
        default=10,
        help="Number of threads to run when running multi-threaded"
    )

    args = parser.parse_args(sys.argv[1:])

    if args.study_ids is not None:
        args.study_ids.close()
        args.study_ids = args.study_ids.name

    fhir_client = FhirClient(host_config[args.env])
    purgery = ResourceDeleter(fhir_client, threaded=args.threaded, max_queue_size=10000, thread_count=args.thread_count)
    if not args.delete_files_by_tag:
        study_ids = purgery.load_studyids(args.study_ids)

    if args.study_name is None:
        print("The following study IDs are available for that server:")
        print("\t"+ "\n\t".join(study_ids))
        sys.exit(1)
    
    if not args.delete_files_by_tag and args.study_name not in study_ids:
        print(f"Unable to find the study ID: {study_name} in the file, {args.study_ids} for the server {args.env}")
        sys.exit(1)
    
    if args.resource is None or len(args.resource) == 0:
        args.resource = ['ALL']
    
    #pdb.set_trace()
    if args.delete_files_by_tag:
        purgery.delete_resources_by_tag(args.study_name, resource_list = args.resource)
    else:
        purgery.delete_resources(args.study_name, resource_list = args.resource)

    purgery.retry_purge()

    # Launch anything that was lingering in the queue
    purgery.cleanup_threads()

    for resource in resource_order:
        qry = f"{resource}?_tag={args.study_name}&_summary=count"
        
        response = fhir_client.get(qry, except_on_error=False)   
        if 'issue' in response.response:
            if "_summary argument is not supported" in response.response['issue'][0]['diagnostics']:
                qry = f"{resource}?_tag={args.study_name}"
                response = fhir_client.get(qry)
                print(f"{resource} : {len(response.entries)}")
        else:
            print(f"{resource} : {response.response['total']}")

        #pdb.set_trace() 
        