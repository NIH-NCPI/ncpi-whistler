#!/usr/bin/env python

"""
ConceptMaps are the FHIR mechanism for translating codes from one vocabulary to 
another. For Whistle, it is used to do the heavy lifting for data 
transformation. To make concept maps simpler, the code in this module will 
take a basic CSV file with columns suitable for both sides of a system 
translation (i.e. code, system, display) and build out the relevant ConceptMap. 

We'll use this to translate everything from Race/Ethnicity to values associated
with disease or labs. One advantage of using this approach is that the familiar
layout of tabular data makes it easy for non-programmers to contribute to the
mapping of data to office codes like LOINC, SNOMED, Mondo or HPO. 

To also enable whistle projector author's to refer to a local code's more 
descriptive representation, this system will also create a 'self' target 
system. 
"""

from csv import DictReader
import json
import sys
from argparse import ArgumentParser, FileType
from pathlib import Path
from collections import defaultdict
from copy import deepcopy

import pdb

"""Convert the harmonization csv file into a FHIR ConceptMap resource for use with Whistle"""

# code,text,code system,local code,display,local code system,comment

class ConceptMap:
    url_base = None
    def __init__(self, name, source_uri, target_uri, title, description):
        """We'll create a single direction CM where the URIs represent the URLs associated with the ValueSets produced to support the CM"""
        self.name = name
        self.source_uri = source_uri
        self.target_uri = target_uri
        self.title = title
        self.description = description

        # source system => target system => source code => (target_codes)
        self.mappings = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        self.sources = defaultdict(dict)
        self.targets = defaultdict(dict)

    def prepare_for_whistle(self, source_id, source_name, source_title, target_id, target_name, target_title):
        """Generate an object suitable as input to Whistle"""

        cm = {
            "name": self.name,
            "title": self.title,
            "description": self.description,
            "sources": {
                "id": source_id,
                "name": source_name,
                "title": source_title,
                "codes": []
            },
            "targets": {
                "id": target_id,
                "name": target_name,
                "title": target_title,
                "codes": []
            },
            "group": []
        }

        for source_system in self.mappings:
            for target_system in self.mappings[source_system]:
                cm["group"].append({
                    "source": source_system,
                    "target": target_system,
                    "element": []
                })
                for source_code in self.mappings[source_system][target_system]:
                    source = self.sources[source_system][source_code]
                    element = {
                        "code": source['code'],
                        "display": source['display'],
                        "target": []
                    }
                    for target_code in self.mappings[source_system][target_system][source_code]:
                        target = self.targets[target_system][target_code]
                        element['target'].append({
                            "code": target['code'],
                            "display": target['display'],
                            "equivalence": "equivalent"
                        })
        return cm

    def generate_cm(self):
        cm = {
            "resourceType": "ConceptMap",
            "url": f"{ConceptMap.url_base}/conceptmap/dd/{self.name}",
            "name": self.name,
            "identifier": self.identifier(),
            "title": self.title,
            "status": "draft",
            "experimental": False,
            "description": self.description,
            "sourceUri": self.source_uri,
            "targetUri": self.target_uri,
            "group": []
        }        

        for source_system in self.mappings:
            for target_system in self.mappings[source_system]:
                cm["group"].append({
                    "source": source_system,
                    "target": target_system,
                    "element": []
                })
                for source_code in self.mappings[source_system][target_system]:
                    source = self.sources[source_system][source_code]
                    element = {
                        "code": source['code'],
                        "display": source['display'],
                        "target": []
                    }
                    for target_code in self.mappings[source_system][target_system][source_code]:
                        target = self.targets[target_system][target_code]
                        element['target'].append({
                            "code": target['code'],
                            "display": target['display'],
                            "equivalence": "equivalent"
                        })

    def identifier(self):
        assert(ConceptMap.url_base is not None)

        return {
            "system": f"{ConceptMap.url_base}/data-dictionary/cm/",
            "value": self.name
        }

    def add_mapping(self, source, target):
        """Each coding should have: system, code and display"""
        self.sources[source['system']][source['code']] = source
        self.targets[target['system']][target['code']] = target
        self.mappings[source['system']][target['system']][source['code']].add(target['code'])

    def source_valueset(self, id, name, title):
        return self.generate_valueset(self.sources, id, name, title)

    def target_valueset(self, id, name, title):
        return self.generate_valueset(self.targets, name, title)

    def generate_valueset(self, codings, id, name, title):
        # Return the valueset associated with the source data
        vs = {
            "resourceType": "ValueSet",
            "id": id,
            "url": f"{ConceptMap.url_base}/data-dictionary/vs/{id}",
            "name": name,
            "title": title,
            "status": "draft",
            "experimental": False,
            "publisher": "NCPI FHIR Working Group",
            "expansion": {
                "contains": []
            }
        }

        for system in codings:
            for code in codings[system]:
                coding = codings[system][code]
                vs['expansion']['contains'].append({
                    "code": coding['code'],
                    "system": coding['system'],
                    "display": coding['display']
                })

        return vs

def BuildConceptMap(csvfilename, curies):
    # We'll assume that we only want to filename with any path/dot information
    name_prefix =csvfilename.split("/")[-1].split(".")[0]
    outname = ".".join(csvfilename.split(".")[0:-1]) + ".json"

    # Skip this step if the json file is newer
    if not Path(outname).exists() or \
        (Path(csvfilename).stat().st_mtime > Path(outname).stat().st_mtime):

        with open(csvfilename, 'rt') as f:
            reader = DictReader(f, delimiter=',', quotechar='"')

            # Make sure the field names are uniform. Just ignore case
            reader.fieldnames = [x.lower() for x in reader.fieldnames]

            # Local system => [dictionary with all columns for easy construction of the group/element]
            mappings = defaultdict(lambda: defaultdict(list))

            rowcount = 0
            for row in reader:
                rowcount += 1
                if row.get('code system') is None:
                    row['code system'] = ""
                mappings[row['local code system']][row['code system']].append(row)
                if row['code system'].strip() != "":
                    mappings[row['local code system']][""].append(row)
            
            concept_map = {
                "id": name_prefix,
                "resourceType": "ConceptMap",
                "version": "v1",
                "group": []
            }

            for source in mappings.keys():
                for target in mappings[source].keys():
                    the_target = target

                    display_kw = "text"
                    rdisplay_kw = 'display'

                    if the_target.strip() == "":
                        the_target = 'self'
                        rdisplay_kw = 'text'

                    element = {
                        "source": source,
                        "target": the_target,
                        "element": []
                    }

                    curie = ""
                    if the_target in curies:
                        curie = curies[the_target] + ":"

                    prev_code = None
                    for elm in mappings[source][target]:
                        local_code = elm['local code']

                        if local_code != prev_code:
                            prev_code = local_code

                            element['element'].append({
                                "code": local_code,
                                "display": elm[display_kw],
                                "target": []
                            })
                        if target == the_target and the_target != "self":
                            element['element'][-1]['target'].append({
                                "code": f"{curie}{elm['code']}",
                                "display": elm[rdisplay_kw],
                                "equivalence": 'equivalent'
                            })
                        else:
                            element['element'][-1]['target'].append({
                                "code": local_code,
                                "display": elm[rdisplay_kw],
                                'equivalence': 'equivalent'
                            })

                    concept_map['group'].append(element)
            
            with open(outname, mode='wt') as f:
                f.write(json.dumps(concept_map, indent=2))

def Exec(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = ArgumentParser()
    parser.add_argument("csvfile",
                        type=FileType('rt'),
                        nargs='+',
                        help="CSV containing the mappings of local values to codes to be used in FHIR output")
    args = parser.parse_args(argv)

    if len(args.csvfile) < 1:
        sys.stderr("You must provide one or more CSV files to transform.")
        sys.exit(1)

    for csvfile in args.csvfile:
        BuildConceptMap(csvfile)

        
