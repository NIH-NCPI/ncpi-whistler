# Questionnaire and Questionnaire Responses

"""
This class will generate the whistle code to process row-level data as QuestionnaireResponses. 

"""
from wstlr import TableType, determine_table_type

from wstlr.extractor import ObjectifyDD, fix_fieldname
from wstlr.sourcedata.sourcedata_base import srcd_questionnaire_def_base
from wstlr.sourcedata import (
    _string_types,
    _integer_types,
    _float_types,
    identifier_columns,
    MissingKeyColumns,
    DdTable,
    DdVariable,
)

from pathlib import Path
from yaml import safe_load
import sys
from argparse import ArgumentParser, FileType
from string import Template

from jinja2 import Environment
from wstlr.config import Configuration
import pdb


def exec():
    parser = ArgumentParser(
        description="Generate whistle code that will produce source data as QuestionnaireResponse."
    )
    parser.add_argument(
        "config",
        nargs="+",
        type=FileType("rt"),
        help="Dataset YAML file with details used to build the necessary code.",
    )
    parser.add_argument(
        "--no-profiles",
        action="store_true",
        help="""By default, resource code will use NCPI profiles. """
        """--no-profile indicates to use only bare FHIR """
        """resources and thus, don't require the NCPI IG to be"""
        """loaded in order for the resources to validate""",
    )
    args = parser.parse_args(sys.argv[1:])
    whistle_context = {"profiles": not args.no_profiles}
    for config_file in args.config:
        config = Configuration(config_file)
        # config = safe_load(config_file)

        if config.projector_lib:
            whistle_src_dir = Path(config.projector_lib)
        else:
            print(
                f"There is no whistle source directory, projector_lib,  specified in the configuration file. Writing whistle code to 'projector'. "
            )
            whistle_src_dir = Path("projector")
        whistle_src_dir.mkdir(exist_ok=True, parents=True)

        consent_group = None
        if config.consent_group:
            consent_group = config.consent_group.code

        files_created = []
        dd_codesystems = {}

        # We'll keep the data portions separately from the process part so that
        # the process functions will be there at the very bottom of the file for
        # clarity
        data_functions = []
        process_functions = []
        entry_fns = []

        dd_table_data = config.study_dd.tables
        """
        for category in config["dataset"].keys():
            if "data_dictionary" in config["dataset"][category]:
                table_type = determine_table_type(config["dataset"][category])

                with open(
                    config["dataset"][category]["data_dictionary"]["filename"],
                    "rt",
                    encoding="utf-8-sig",
                ) as f:
                    delimiter = ","
                    if "delimiter" in config["dataset"][category]["data_dictionary"]:
                        delimiter = config["dataset"][category]["data_dictionary"][
                            "delimiter"
                        ]

                    dd, cs_values = ObjectifyDD(
                        config["study_id"],
                        consent_group,
                        category,
                        f,
                        dd_codesystems,
                        config["dataset"][category]["data_dictionary"].get("colnames"),
                        delimiter=delimiter,
                    )

                    dd_table_data.append(
                        DdTable(
                            dd,
                            config["dataset"][category],
                            fix_fieldname(config["id_colname"]),
                        )
                    )
        """
        template_dir = Path(__file__).resolve().parent / "templates"
        with open(template_dir / "questionnaires.wstl", "rt") as template_file:
            jinja_env = Environment()  # trim_blocks = True) #, lstrip_blocks = True)

            template = jinja_env.from_string(template_file.read())
            with open(
                whistle_src_dir / "source_data_questionnaires.wstl", "wt"
            ) as outf:
                print(outf.name)
                outf.write(
                    template.render(
                        tables=dd_table_data,
                        TableType=TableType,
                        use_profiles=args.no_profiles == False,
                    )
                )

        print(
            "Please add the function, BuildRawDataQR to your main transform library function"
        )
