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
from argparse import ArgumentParser, FileType
from wstlr.bundle import Bundle, ParseBundle, RequestType

def ReportError(is_error, resource, message):
    if is_error:
        print(pformat(resource))
        print(message)
        sys.exit(1)

def CheckForUse(identifiers):
    official_count = 0
    for idnt in identifiers:
        if idnt.get('use') == 'official':
            official_count += 1
    if official_count != 1:
        print(identifiers)
        pdb.set_trace()
    return official_count == 1

class ResourceInspector:

    def __init__(self, require_official):
        # ResourceType => Set(ids)
        self.identifiers = defaultdict(set)
        self.require_official = require_official

    def check_identifier(self, group_name, resource):
        ReportError('identifier' not in resource, resource, "There is no identifier present in this resource")
        ReportError('meta' not in resource or 'tag' not in resource['meta'], resource, "There is no meta.tag present.")
        ReportError('resourceType' not in resource, resource, "There is no resourceType specified in this resource")

        if self.require_official:
            ReportError(not CheckForUse(resource['identifier']), resource, "There is no 'use: official' as requested by portal team")

        identifier = resource['identifier'][0]
        resourcetype = resource['resourceType']
        if 'system' not in identifier:
            print(identifier)
            pdb.set_trace()
        idval = f"{identifier['system']}:{identifier['value']}"
        #print(idval)

        if idval in self.identifiers[resourcetype]:
            print(resourcetype)
            print(self.identifiers[resourcetype])
            pdb.set_trace()

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

    for result_file in args.file:
        ParseBundle(result_file, [resource_inspector.check_identifier])