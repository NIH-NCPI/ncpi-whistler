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

def ObjectifyHarmony(harmony_csv, curies):
    # source system => target system => source code => (target_codes)
    mappings = {}       # defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    sources = defaultdict(dict)
    targets = defaultdict(dict)

    # We need to aggregate the value-set components by system, parent and table so that we can 
    # create them properly within fhir
    vs_sources = defaultdict(list)

    if curies is None:
        curies = {}

    with open(harmony_csv, 'rt') as f:
        reader = DictReader(f, delimiter=',', quotechar='"')

        redundant_notice = defaultdict(lambda: defaultdict(lambda: 0))
        for line in reader:
            if line['table_name'].strip() != "":
                #print(line.keys())
                local_cs = line['local code system']
                if local_cs not in mappings:
                    mappings[local_cs] = {
                        "source_cs": local_cs,
                        "parent": line['parent_varname'],
                        "table": line['table_name'],
                        "group": {}
                    }

                target_cs = line['code system']
                if target_cs not in mappings[local_cs]['group']:
                    mappings[local_cs]['group'][target_cs] = {
                        "target_cs": target_cs,
                        "codes": {}
                    }
                
                local_code = line['local code']
                if local_code not in mappings[local_cs]['group'][target_cs]['codes']:
                    mappings[local_cs]['group'][target_cs]['codes'][local_code] = {
                        "code": local_code,
                        "system": "",
                        "table": line['table_name'],
                        "parent": line['parent_varname'],
                        "display": line['text'],
                        "table_name": line['table_name'],
                        "parent_varname": line['parent_varname'],
                        "target_codes": {}
                    }
                target_code = line['code']
                curie = ""
                if  target_cs in curies:
                    curie = curies[target_cs] + ":"

                # We do have some redundant rows where the CDE has two variables but the dataset doesn't specify 
                # at that detail. We'll just keep the last and hope they are identical
                if target_code in mappings[local_cs]['group'][target_cs]['codes'][local_code]['target_codes']:
                    redundant_notice[f"{local_cs}:{local_code}"][f"{target_cs}:{target_code}"] += 1
                    
                mappings[local_cs]['group'][target_cs]['codes'][local_code]['target_codes'][target_code] = {
                    "code": line['code'],
                    "display": line['display'],
                    "system": line['code system'],
                    "table": "",
                    "parent": ""
                }

                vss_key = f"{local_cs}:{line['table_name']}:{line['parent_varname']}"
                vs_sources[vss_key].append({
                    "code": local_code,
                    "display": line['text']
                })
                sources[local_cs][local_code] = {
                    "code": local_code,
                    "display": line['text'],
                    "system": "",
                    "table": line['table_name'],
                    "parent": line['parent_varname']
                }
                targets[target_cs][target_code] = {
                    "code": f"{curie}{target_code}",
                    "display": line['display'],
                    "system": target_cs,
                    "table": "",
                    "parent": ""
                }
        if len(redundant_notice) > 0:
            print(f"The following mappings were found to be duplicated")
            printed = 0
            for k in redundant_notice: 
                printed += 1
                if printed < 10:
                    print(f"{k}: {', '.join(redundant_notice[k].keys())}")
            if len(redundant_notice) > 10:
                print(f"And {len(redundant_notice) - 10} more.")

    cm_obj = {
        # Stuff to build the two value sets
        "source_codes": [],
        "target_codes": [],
        "mappings": []
    }
    #pdb.set_trace()
    for vs_key in vs_sources:
        csystem, tablename, parentvarname = vs_key.split(":")
        curie = ""
        if csystem in curies:
            curie = curies[csystem] + ":"
        
        cm_obj['source_codes'].append({
            "system": csystem,
            "table_name": tablename,
            "parent_varname": parentvarname,
            "codes":[]
        })

        for coding in vs_sources[vs_key]:
            code = f"{curie}{coding['code']}"
            cm_obj['source_codes'][-1]['codes'].append({
                "code": code,
                "display": coding['display']
            })
    for target_cs in targets:
        curie = ""
        cm_obj['target_codes'].append({
            "system": target_cs,
            "table_name": "",
            "parent_varname": "",
            "codes": []
        })
        #if  target_cs in curies:
        #    curie = curies[target_cs] + ":"
        for code in targets[target_cs]:
            coding = deepcopy(targets[target_cs][code])
            coding['code'] = f"{curie}{coding['code']}"

            cm_obj['target_codes'][-1]['codes'].append({
                "code": coding['code'],
                "display": coding['display']
            })

    for local_cs in mappings:
        for target_cs in mappings[local_cs]['group']:
            local_mapping = {
                "source": local_cs,
                "table": mappings[local_cs]['table'],
                "parent": mappings[local_cs]['parent'],
                "target": target_cs,
                "element": []
            }               

            for source_code in mappings[local_cs]['group'][target_cs]['codes']:
                source_mapping = mappings[local_cs]['group'][target_cs]['codes'][source_code]
                element = {
                    "code": source_code,
                    "display": source_mapping['display'],
                    "target": []
                }
                #pdb.set_trace()
                for target_code in source_mapping['target_codes']:
                    target_mapping = source_mapping['target_codes'][target_code]
                    #print(target_mapping)
                    element['target'].append({
                        "code": target_mapping['code'],
                        "display": target_mapping['display']
                    })
                local_mapping['element'].append(element)
            
            #pdb.set_trace()
            cm_obj['mappings'].append(local_mapping)

    return cm_obj

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

    def generate_valueset(self, codings, id, name, title, curies):
        # Return the valueset associated with the source data

        if curies is None:
            curies = {}
        vs = {
            "resourceType": "ValueSet",
            "id": id,
            "url": f"{ConceptMap.url_base}/data-dictionary/vs/{id}",
            "name": name,
            "title": title,
            "status": "draft",
            "experimental": False,
            "publisher": "NCPI FHIR Working Group",
            "compose": {
                "include": []
            }
        }

        for system in codings:
            curie = ""
            if system in curies:
                curie = curies[system] + ":"
            vs['compose']['include'].append({
                "system": system,
                "concept": []
            })
            for code in codings[system]:
                code = f"{curie}{coding['code']}"
                coding = codings[system][code]
                vs['compose']['include'][-1]['concept'].append({
                    "code": code,
                    "display": coding['display']
                })

        return vs

def BuildConceptMap(csvfilename, curies, codesystems=[]):
    # We'll assume that we only want to filename with any path/dot information
    name_prefix =csvfilename.split("/")[-1].split(".")[0]
    outname = ".".join(csvfilename.split(".")[0:-1]) + ".json"

    observed_mappings = set()

    # Skip this step if the json file is newer
    if (not Path(outname).exists()) or \
        (Path(csvfilename).stat().st_mtime > Path(outname).stat().st_mtime):

        with open(csvfilename, 'rt') as f:
            reader = DictReader(f, delimiter=',', quotechar='"')

            # Make sure the field names are uniform. Just ignore case
            reader.fieldnames = [x.lower() for x in reader.fieldnames]

            # Local system => [dictionary with all columns for easy construction of the group/element]
            mappings = defaultdict(lambda: defaultdict(list))

            rowcount = 0
            for row in reader:
                if row.get('code system') is None:
                    row['code system'] = ""

                key = ".".join([
                    row['local code system'],
                    row['local code'],
                    row['code system'],
                    row['code']])

                rowcount += 1

                if key not in observed_mappings:
                    observed_mappings.add(key)

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

                    observed_codes = set()
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
                    
            for cs in codesystems:
                element = None
                # In order to use variable categoricals in the harmony, 
                # we'll need to build support for them. 
                # 
                # The only real gotcha here is that those target URLs
                # have to be real URLs or the resulting output from the
                # harmony calls will fail when submitting to a FHIR server
                if 'varname' in cs:
                    if len(cs['values']) > 0:
                        element = {
                            "source": cs["varname"],
                            "target": cs["url"],
                            "element": []
                        }
                else:
                    element = {
                        "source": cs['table_name'],
                        "target": cs['url'],
                        "element": []
                    }

                if element is not None:
                    for entry in cs['values']:
                        element['element'].append({
                                "code": entry['code'],
                                "display": entry['description'],
                                "target": [{
                                    "code": entry['code'],
                                    "display": entry['description'],
                                    "equivalence": 'equivalent'
                            }]
                        })

                    # After some work exploring alternatives, the solution is this: 
                    #   * We must provide URLs for each code-system to be used by the whistle 
                    #   * We must then make sure all references to these can be properly traced back to 
                    #     those URLs and avoid creating URLs for anything that could ultimately make it 
                    #     into those live harmony files. 
                    #    
                    #     That said, the harmony components themselves (conceptmap and it's source/target
                    #     valuesets) will need URLs and it is safe to create those on the fly within whistle
                    #     since those will no be referenced by anything else
                    concept_map['group'].append(element)

            print(f"Writing Harmony ConceptMap: {outname}")
            with open(outname, mode='wt') as f:
                f.write(json.dumps(concept_map, indent=2))
    return Path(outname).stat().st_mtime

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

        
