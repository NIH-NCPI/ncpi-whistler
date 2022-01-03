"""Help user build out data-dictionaries based on the JSON version of their
whistle input"""

import json
import sys
from argparse import ArgumentParser, FileType
from collections import defaultdict
from pathlib import Path
import csv

import pdb
_codes_produced=defaultdict(int)
def build_code(prefix, varwidth=6):
    global _codes_produced
    _codes_produced[prefix] += 1
    return f"{prefix}{str(_codes_produced[prefix]).zfill(varwidth)}"

class Variable:
    def __init__(self, varname, desc="", code_prefix="q"):
        self.varname = varname 
        self.desc = desc
        self.code_prefix = code_prefix

        # value => # of that value observed
        self.values = defaultdict(int)

        self.datatype = None

        self.min = None
        self.max = None

        # Basically points to various questions, each of 
        # which seem to be able to have different responses
        self.complex_variables = defaultdict(lambda: defaultdict(int))

    def add_value(self, value):
        if type(value) is list:
            for entry in value:
                self.complex_variables[entry['code']][entry['value']] += 1
        else:
            try:
                val = float(value)
                if self.min is None or val < self.min:
                    self.min = val
                elif self.max is None or val > self.max:
                    self.max = val
            except:
                pass
            self.values[value] += 1       

    @classmethod
    def header(cls, writer):
        writer.writerow([
            "VARNAME",
            "VARDESC",
            "TYPE",
            "UNITS",
            "MIN",
            "MAX",
            "VALUES",
            "COMMENT",
            "COUNT_COMMENT"
        ])
    def write(self, writer):
        values = set()


        count_comment = []
        if len(self.values) < 50:
            for value,count in self.values.items():
                if value.strip() != "":
                    #pdb.set_trace()
                    if len(value) > 20:
                        codeval = build_code(self.code_prefix)
                        values.add(f"{codeval}={value}")
                    else:
                        values.add(value)

                if value.strip() == '':
                    value="MISSING"
                count_comment.append(f"{value}={count}")


        if len(self.complex_variables) < 50:
            # We'll capture uniq question:values so that we can have only
            # one present regardless of the number of unique responses there
            # may be to a given set of check-boxes. Those will be different. 
            value_to_code = {}
            for value,entry in self.complex_variables.items():
                for key,count in entry.items():
                    count_comment.append(f"{value}:{key}={count}")
                    if value in value_to_code:
                        codeval = value_to_code[value]
                    else:
                        codeval = build_code(self.code_prefix)
                        value_to_code[value] = codeval
                        values.add(f"{codeval}={value}")

        values = ";".join(list(values))
        count_comment = ";".join(count_comment)

        
        data = [
            self.varname,
            self.desc,
            "",
            "",
            "",
            "",
            values,
            "",
            count_comment
        ]

        if self.datatype is not None:
            data[2] = self.datatype
        if self.min is not None:
            data[4] = self.min
        if self.max is not None:
            data[5] = self.max
        writer.writerow(data)

def exec(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = ArgumentParser()
    parser.add_argument("json_input",
                        type=FileType('rt', encoding='utf-8-sig'),
                        nargs=1,
                        help="The JSON file containing data to be evaluated")
    parser.add_argument("-o", 
                        "--output",
                        type=str,
                        default="output",
                        help="output directory for resulting CSV file")
    parser.add_argument("-p",
                        "--code-prefix",
                        type=str,
                        default="q",
                        help="Short prefix to be used as initial part of code when a sequential code is created for long fieldnames")
    parser.add_argument("-v",
                        "--value-prefix",
                        type=str,
                        default='v',
                        help="Short prefix to be used as the initial part of the value code for lengthy value names")
    args = parser.parse_args()

    output_directory = Path(args.output)
    output_directory.mkdir(parents=True, exist_ok=True)

    content = json.load(args.json_input[0])
    filename_prefix = Path(args.json_input[0].name).stem

    # We don't really want to treat study and code-systems as
    # if they are tables. However, we will attempt to glean
    # some descriptive information from the the study.data-dictionary 
    # chunks
    ignored_components = ['study', 'code-systems']

    # We'll use this to attempt to pull some parts from the 
    # original DD where they overlap
    original_dd = defaultdict(dict)

    current_dd = {}
    if 'study' in content and 'data-dictionary' in content['study']:
        for dd in content['study']['data-dictionary']:
            table_name = dd['table_name']
            for variable in dd['variables']:
                varname = variable['varname']

                var = Variable(variable['varname'], variable['desc'], code_prefix=args.value_prefix)
                var.datatype = variable['type']
                for value in var.values:
                    code = value['code']
                    desc = value['description']

                    if code != desc:
                        code = f"{code}={desc}"

                    var.values[code] = 0

                original_dd[table_name][variable['varname']] = var

    for table_name in content:
        if table_name not in ignored_components:
            current_dd[table_name] = {}
            for participant in content[table_name]:
                for varname, value in participant.items():
                    if varname not in current_dd[table_name]:
                        var = None
                        if varname in original_dd[table_name]:
                            var = original_dd[table_name][varname]
                        else:
                            # We don't get a description from these
                            # json objects. Let's create our own variable names 
                            # and use the real varname as the desc
                            varcode = build_code(args.code_prefix)
                            var = Variable(varcode, varname, code_prefix=args.value_prefix)
                        current_dd[table_name][varname] = var
                    
                    current_dd[table_name][varname].add_value(value)

            outfilename = output_directory / f"{filename_prefix}-{table_name}.csv"
            with outfilename.open('wt') as f:
                print(f"Writing {outfilename}")
                writer = csv.writer(f, delimiter=',', quotechar='"')

                Variable.header(writer)
                for varname,var in current_dd[table_name].items():
                    #pdb.set_trace()
                    var.write(writer)
    

