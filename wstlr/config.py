"""
Basic representation of the configuration components associated with a whistler
project. 
"""

from wstlr import die_if
from yaml import safe_load

from wstlr.dd.json_parser import JsonParser
from wstlr.dd.csv_parser import CsvParser
from wstlr.dd.study import DdStudy
from wstlr.dd.table import DdTable
from wstlr import fix_fieldname

from pathlib import Path
import pdb


class Configuration:
    def __init__(self, cfgfile):
        self.filename = cfgfile.name

        self.configuration = safe_load(cfgfile)
        self.host = None

        if self.configuration.get("id_colname") is not None:
            DdTable.default_subject_id(fix_fieldname(self.configuration["id_colname"]))

        # This is the data dictionary study object
        self.study_dd = None

        if "anvil_data_model" in self.configuration:
            model_config = self.configuration["anvil_data_model"]
            die_if(
                "filename" not in model_config,
                "anvil_data_model config is missing property, 'filename'.",
            )

            jsonp = JsonParser(
                filename=model_config["filename"],
                tables_path="tables",
                columns_path="columns",
                colnames=model_config.get("colnames", {}),
            )

            self.study_dd = jsonp.study

        else:
            self.study_dd = DdStudy(
                self.study_id, self.study_desc, url_base=self.dd_prefix
            )

            csvp = None
            for table_name, table in self.dataset.items():
                if "data_dictionary" in table and table.get("hidden") != True:
                    csv_filename = table["data_dictionary"]["filename"]
                    colnames = table["data_dictionary"].get("colnames", {})

                    if csvp is None:
                        csvp = CsvParser(
                            csv_filename,
                            self.study_id,
                            self.study_desc,
                            table_name=table_name,
                            colnames=colnames,
                            url_base=self.dd_prefix,
                        )
                    else:
                        csvp.open(csv_filename, name=table_name, colnames=colnames)

            self.study_dd = csvp.study

    def from_config(self, key, default=None, required=False):
        die_if(
            required and key not in self.configuration,
            f"Required configuration parameter, '{key}' is missing from file, "
            f"'{self.filename}'.",
        )
        return self.configuration.get(key, default)

    def parse_args(self, args):
        self.host = args.host
        if args.env is not None:
            die_if(
                args.env not in self.env,
                f"The environment, {args.env}, is not configured in "
                f"{self.filename}.",
            )

            self.host = self.env[args.env]

    @property
    def study_title(self):
        return self.from_config("study_title", required=True)

    @property
    def study_desc(self):
        return self.from_config("study_desc")

    @property
    def url(self):
        return self.from_config("url")

    @property
    def dd_prefix(self):
        if "dd_prefix" in self.configuration:
            return self.from_config("dd_prefix")
        return self.identifier_prefix

    @property
    def harmony_prefix(self):
        return self.from_config("harmony_prefix", required=True)

    @property
    def code_harmonization(self):
        return self.from_config("code_harmonization")

    @property
    def config(self):
        return self.from_config("config", required=False)

    @property
    def identifier_prefix(self):
        return self.from_config("identifier_prefix", required=True)

    @property
    def id_colname(self):
        return self.from_config("id_colname")

    @property
    def projections(self):
        return self.from_config("projections", required=True)

    @property
    def whistle_src(self):
        return self.from_config("whistle_src", required=True)

    @property
    def code_harmonization_dir(self):
        return self.from_config("code_harmonization_dir", default="harmony")

    @property
    def annotations(self):
        return self.from_config("annotations", default={})

    @property
    def curies(self):
        return self.from_config("curies", default={})

    @property
    def projector_lib(self):
        return self.from_config("projector_lib", default="projector")

    @property
    def env(self):
        return self.from_config("env")

    @property
    def active_tables(self):
        return self.from_config("active_tables", {"ALL": True})

    @property
    def dataset(self):
        return self.from_config("dataset", required=True)

    @property
    def study_id(self):
        return self.from_config("study_id", required=True)

    @property
    def study_accession(self):
        return self.from_config("study_accession")

    @property
    def require_official(self):
        return self.from_config("require_official", default=False)

    @property
    def environment(self):
        return self.from_config("env")

    @property
    def output_filename(self):
        return self.from_config("output_filename", default=self.study_id.lower())

    @property
    def publisher(self):
        return self.from_config("publisher")

    @property
    def remote_data_access(self):
        return self.from_config("remote_data_access")

    @property
    def study_sponsor(self):
        return self.from_config("study_sponsor")

    @property
    def consent_group(self):
        return self.from_config("consent_group")

    @property
    def fhir_id_patterns(self):
        return self.from_config("fhir_id_patterns")

    @property
    def resource_list(self):
        return self.from_config("resource_list")
