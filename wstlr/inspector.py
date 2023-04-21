"""Inspect the fhir resources to identify potential issues before trying to submit. 

There are some problems, such as having the same resource identifiers that are consumed 
in parallel that can fail to be recognized due to the fact that all recognize that the 
no id exists so they all post new resources, resulting in duplicate records. 

These types of issues do represent potential errors in the whistle code. 

"""

from collections import defaultdict
from pprint import pformat
import pdb
from pathlib import Path
import json
import sys
from wstlr.module_summary import ModuleSummary
from argparse import ArgumentParser, FileType
from wstlr.bundle import Bundle, ParseBundle, RequestType
from rich import print

def ReportError(is_error, resource, message):
    if is_error:
        print(pformat(resource))
        print(message)
        sys.exit(1)

def CheckForUse(identifiers):
    official_count = 0

    # ConceptMap can only have one identifier (possibly others as well)
    if type(identifiers) is list:
        for idnt in identifiers:
            if idnt.get('use') == 'official':
                official_count += 1
    else:
        if identifiers.get('use') == 'official':
            official_count = 1
    if official_count != 1:
        print(identifiers)
        pdb.set_trace()
    return official_count == 1


class ObservationInspector:
    def __init__(self):
        pass

    def inspect(self, group_name, resource):

        ReportError('resourceType' not in resource, resource, "There is no resourceType specified in this resource")

        if resource['resourceType'] == "Observation":
            ReportError('code' not in resource, resource, "There is no code present in this resource")

class ResourceInspector:
    def __init__(self, require_official):
        # ResourceType => Set(ids)
        self.identifiers = defaultdict(set)
        self.require_official = require_official

    def check_identifier(self, group_name, resource):
        if 'resourceType' not in resource:
            print(resource)
            print("No resourceType was found. As such, this is not a valid resource")
            pdb.set_trace()

        ReportError('resourceType' not in resource, resource, "There is no resourceType specified in this resource")

        # CMs can only have one identifier, which has all sorts of downstream issues with the system...so, skipping them for
        # now
        if resource['resourceType'] not in ['ConceptMap']:
            ReportError('identifier' not in resource, resource, "There is no identifier present in this resource")
        ReportError('meta' not in resource or 'tag' not in resource['meta'], resource, "There is no meta.tag present.")

        if self.require_official:
            ReportError(not CheckForUse(resource['identifier']), resource, "There is no 'use: official' as requested by portal team")

        identifier = resource['identifier']
        if type(identifier) is list:
            identifier = resource['identifier'][0]
        
        resourcetype = resource['resourceType']
        if 'system' not in identifier:
            print(resource)
            print(identifier)
            pdb.set_trace()
        idval = f"{identifier['system']}:{identifier['value']}"
        #print(idval)

        if idval in self.identifiers[resourcetype]:
            print(resourcetype)
            print(self.identifiers[resourcetype])
            #pdb.set_trace()

        ReportError(idval in self.identifiers[resourcetype], resource, f"The following identifier appears multiple times: \n{pformat(identifier)}")
        self.identifiers[resourcetype].add(idval)

def exec():
    parser = ArgumentParser(
        description="Run inspection on an existing whistle output file."
    )
    parser.add_argument(
        "-r",
        "--require-official",
        type=bool,
        default=True
    )
    parser.add_argument(
        "file",
        nargs='+',
        type=FileType('rt'),
        help="JSON output from Whistle to be inspected.",
    )

    args = parser.parse_args(sys.argv[1:])
    resource_inspector = ResourceInspector(require_official=args.require_official)
    obs_inspector = ObservationInspector()
    summary = ModuleSummary()
    for result_file in args.file:
        modules = set(ParseBundle(result_file, [resource_inspector.check_identifier, obs_inspector.inspect, summary.summary]))

    summary.print_summary()
