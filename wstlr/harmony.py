"""Provide a way to extract data from the data-dictionary of the JSON objects
and build out a skeleton data-harmonization csv file to be filled out with
appropriate codes

We will stick with CSV so that we can hand the file over to other folks to fill out. 

In the future, we can add consumers that will employ NLP or any other technology to 
fill in the gaps. 
"""

import json
import csv
from copy import deepcopy

import pdb

class CodeEntry:
    def __init__(self, code, system, display, comment):
        self.code = code
        self.system = system
        self.display = display
        self.comment = comment
    


class Variable:
    def __init__(self, ddvar, system):
        self.varname = ddvar['varname']
        self.desc = ddvar['desc']
        self.ddcontent = deepcopy(ddvar)
        self.parent = ddvar.get('parent')
        self.system = system
        # System is how the whistle will identify which code system the code is from
        # for top level variable names, this will be the table name. 
        # for value level names, this will be the variable name to which the value is assigned
        self.entry_main = CodeEntry(self.varname, system, ddvar['desc'], "")
        self.annotations = []

    @classmethod
    def writeheader(cls, writer):
        # code,text,code system,local code,display,local code system,comment
        writer.writerow([
            'local code',
            'text',
            'local code system',
            'code',
            'display',
            'code system',
            'comment'
        ])

    def writerow(self, writer, annotation=None):
        data = [self.varname, self.entry_main.display, self.entry_main.system]
        if annotation is not None:
            data+= [annotation.code, annotation.display, annotation.system, annotation.comment]

        writer.writerow(data)

    def writerow_annotations(self, writer):
        if len(self.annotations) > 0:
            for annotation in self.annotations:
                self.writerow(writer, annotation)
        else:
            self.writerow(writer)

    def add_annotation(self, code, display, system, comment):
         self.annotations.append(CodeEntry(code, system, display, comment))



ignore_these_values = set(['yes','no'])

def ParseJSON(input_json, out_csv_file, variable_consumers=[], filter=[]):
    """Iterate over each variable in each of the 'tables' and pass them to each of the variable consumers. If nothing is found, an entry with only the local variable details will be added to the file"""
    writer = csv.writer(out_csv_file, delimiter=',', quotechar='"')
    content = json.load(input_json)
    Variable.writeheader(writer)

    for table in content['study']['data-dictionary']:
        table_name = table['table_name']

        for variable in table['variables']:
            v = Variable(variable, table_name)

            filtered_out = False
            for flt in filter:
                filtered_out = filtered_out or flt(v)

            if not filtered_out:
                for consumer in variable_consumers:
                    consumer(v)

                v.writerow(writer)

                # Now lets look at the values:
                for value in variable['values']:
                    vdesc = value['description']



                    if vdesc.lower() not in ignore_these_values:
                        try:
                            float(vdesc)
                            # Basically, if it's a number then it's not something we will worry about
                        except:
                            v = Variable({"varname":value['code'], "desc":value["description"]}, variable['varname'])
                            
                            if v.system in ['consents']:
                                #pdb.set_trace()
                            filtered_out = False
                            for flt in filter:
                                filtered_out = filtered_out or flt(v)

                            if not filtered_out:
                                for consumer in variable_consumers:
                                    consumer(v)

                                v.writerow_annotations(writer)

            
