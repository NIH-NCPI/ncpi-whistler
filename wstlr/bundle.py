#!/usr/bin/env python
"""Handle bundles from within the wstlr framework. This includes
   parsing the bundles generated by whistle itself and potentially
   prepping those bundles for loading into a FHIR server as 
   transaction bundles

   Resource Consumers - These are functors which accept two parameters,
   a FHIR resource and a corresponding group name. It is up to the 
   consumer to determine what to do with each resource passed 
   (if anything at all).    

   example: 
   
def ConsumeResource(group_name, resource):
    # Do stuff here
"""

import json
from enum import Enum
from copy import deepcopy
from argparse import ArgumentParser, FileType
from collections import OrderedDict
from wstlr import get_host_config
import sys
from pathlib import Path
from rich import print
from rich.progress import track
import pdb


def ParseBundle(bundle_file, resource_consumers):
    """Iterate over each resource inside the bundle and pass those
    resources to each resource_consumers."""

    content = json.load(bundle_file) 
    if content is not None:

        modules = list(content.keys())

        # When we switched over to using global IDs for Patients, this created
        # issues  with
        if "patient" in modules:
            modules = ["patient"] + list(set(modules) - set(["patient"]))

        print(f"Loading content from file, {bundle_file.name}")
        # pdb.set_trace()
        # We expect there to be at least one key that points
        # to an array of resource records. If there are more
        # that is fine. Each will be processed independently
        # and the key will be passed as the "resource_group"
        for resource_group in modules:
            for resource in track(
                content[resource_group],
                f"Processing {len(content[resource_group])} resources for {resource_group}",
            ):
                # for resource in content[resource_group]:
                # print(resource)
                # print(f"{resource_group}:{resource}")
                for consumer in resource_consumers:
                    consumer(resource_group, resource)
        return content.keys()

    else:
        print(f"The file, {bundle_file.name}, appears to be empty.")
        sys.exit(1)


class RequestType(Enum):
    PUT = 1
    POST = 2


class Bundle:
    """Update the bundle created by whistle to be a valid transaction bundle"""

    def __init__(
        self, file_prefix, bundle_id, target_service_url, request_type=RequestType.PUT
    ):
        self.file_prefix = file_prefix
        self.filename = None
        self.write_comma = False
        self.bundle_id = bundle_id
        self.cur_group = None
        self.bundle = None
        self.bundle_size = 0
        self.request_type = request_type
        self.target_service_url = target_service_url
        self.verb = "PUT"
        self.max_records = 15000
        self.file_index = 0
        self.records_written = 0
        self.urls_seen = set()
        if request_type == RequestType.POST:
            self.verb = "POST"

    def init_bundle(self, group):
        if self.bundle is not None:
            self.close_bundle()

        self.file_index += 1
        if group == "entry":
            self.filename = f"{self.file_prefix}-{self.file_index:05d}.json"
        else:
            self.filename = f"{self.file_prefix}-{group}-{self.file_index:05d}.json"

        if self.cur_group != group:
            self.bundle_size = 0
            self.file_index = 0
            self.max_records = 15000

            # Cheap fix to get rid of duplicate entries while I finish testing
            # capabilities of transaction bundles
            self.urls_seen = set()
        self.records_written = 0
        self.cur_group = group

        self.bundle = open(self.filename, "wt")

        self.write_comma = False
        self.bundle.write(
            """{
    "resourceType": "Bundle",
    "id": \""""
            + self.bundle_id
            + """\",
    "type": "transaction",
    "entry": [
"""
        )

    def consume_resource(self, group, resource):
        response = deepcopy(resource)

        if group != self.cur_group or self.max_records <= self.records_written:
            self.init_bundle(group)
        if self.bundle:

            # For now, let's just skip the ID so that it works in a more general sense
            verb = self.verb
            if "resourceType" not in resource or "id" not in resource:
                pass
                # print(resource.keys())
                # pdb.set_trace()

            if "id" in resource and self.request_type == RequestType.PUT:
                id = resource["id"]
                destination = f"{resource['resourceType']}/{resource['id']}"
            else:
                verb = "POST"
                destination = f"{resource['resourceType']}"
                id = resource["identifier"][0]["value"]

            # pdb.set_trace()
            resource_data = json.dumps(resource)

            self.bundle_size += 1
            self.records_written += 1

            full_url = f"""{self.target_service_url}/{resource['resourceType']}/{id}"""

            if full_url not in self.urls_seen:
                if self.write_comma:
                    self.bundle.write(",")
                self.write_comma = True
                self.urls_seen.add(full_url)
                self.bundle.write(
                    """    {
      "fullUrl": \""""
                    + full_url
                    + """\",
      "resource": """
                    + resource_data
                    + """,
      "request": {
          "method": \""""
                    + verb
                    + """\",
          "url": \""""
                    + destination
                    + """\"
      }
    }"""
                )
            else:
                print(f"Skipping duplicate entry for {full_url}")
        return response

    def close_bundle(self):
        print(
            f"Closing Bundle {self.filename} with {self.records_written} entries ({self.bundle_size} records so far)."
        )
        if self.bundle:
            self.bundle.write(
                """
  ]
}"""
            )
            self.bundle.close()


def exec():
    host_config = get_host_config()
    # Just capture the available environments to let the user
    # make the selection at runtime
    env_options = sorted(host_config.keys())

    parser = ArgumentParser(
        description="Convert Whistle generated bundle into a proper transaction bundle."
    )
    parser.add_argument(
        "-e",
        "--env",
        choices=env_options,
        default=env_options[0],
        help=f"Remote configuration to be used to access the FHIR server",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="output/whistle-output/",
        help="Directory for transaction bundle to be written (file name will be based on source filename)",
    )
    parser.add_argument(
        "filename", nargs="+", type=FileType("rt"), help="JSON file from whistle output"
    )
    args = parser.parse_args(sys.argv[1:])

    for fn in args.filename:
        fname = f"{Path(fn.name).stem}-transaction.json"
        outfilename = Path(args.output) / fname

        bundle = Bundle(str(outfilename), fname, args.env)
        ParseBundle(fn, [bundle.consume_resource])
