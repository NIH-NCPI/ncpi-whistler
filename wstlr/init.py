"""
Whistler will initialize a project by possibly extracting core whistle
code including data-dictionary terminology and core functions. 
"""

import sys
from packaging import version
from yaml import safe_load

from wstlr import die_if
import pdb

from argparse import ArgumentParser, FileType
from jinja2 import Template

# The files() functionality only works in 3.10 and later but the backport
# works fine for earlier versions
if version.parse(sys.version.split(" ")[0]) < version.parse("3.10"):
    try:
        from importlib_resources import files
    except:
        die_if(
            True,
            f"""You are using python version, """
            f"""{sys.version.split(" ")[0]}, which lacks certain key """
            """functionality.\n\n"""
            """To proceed using this program, you must install the backport. """
            """If you are using\npip, you can do so with the following """
            """command. \n\npip install importlib_resources\n""",
        )
else:
    from importlib.resources import files

import shutil
from pathlib import Path


def copy_files(module_path, dest, context):
    """Copy files from the library module to the user's project directory"""
    for f in files(module_path).iterdir():
        template = Template(f.open("rt").read())
        with (dest / f.name).open("wt") as outf:
            print(outf.name)
            outf.write(template.render(context=context) + "\n")


def module_names(root_path="wstlr.wlib"):
    """List the different modules available so we can make them a choice in"""
    """the CLI tool. """
    module_names = {}

    wstlr_path = Path(__file__).resolve().parent

    for d in files(root_path).iterdir():
        rel_path = str(d.relative_to(wstlr_path)).replace("/", ".")
        module_names[Path(d).name] = f"wstlr.{rel_path}"

    return module_names


def exec(args=None):
    if args is None:
        args = sys.argv[1:]

    module_options = module_names()

    parser = ArgumentParser(
        description="""init-play will auto-generate """
        """default whistle code that can help speed production """
        """of ETL along""",
        epilog="""The dataset configuration is a simple YAML file"""
        """containing basic details associated with the study, it's """
        """files and harmony components.""",
    )

    # We'll permit multiple configs in case the user has different projection
    # directories associated with those different configurations
    parser.add_argument(
        "config",
        type=FileType("rt", encoding="utf-8-sig"),
        nargs="+",
        help="""YAML file containing dataset configuration details.""",
    )
    parser.add_argument(
        "-m",
        "--modules",
        action="append",
        choices=list(module_options.keys()) + ["ALL"],
        default=[],
        help="""Indicate which modules to provide (ALL by default) """,
    )
    parser.add_argument(
        "--no-profiles",
        action="store_true",
        help="""By default, resource code will use NCPI profiles. """
        """--no-profile indicates to use only bare FHIR """
        """resources and thus, don't require the NCPI IG to be"""
        """loaded in order for the resources to validate""",
    )

    args = parser.parse_args(args=args)

    if len(args.modules) == 0 or "ALL" in args.modules:
        args.modules = list(module_options.keys())

    if args.modules != ["ALL"] and "core" not in args.modules:
        print(
            """The module, core, is required for all other modules. Adding it to the list\n"""
        )
        args.modules.append("core")

    print(f"Selected modules: {','.join(args.modules)}\n")

    whistle_context = {"profiles": not args.no_profiles}

    for config_file in args.config:
        config = safe_load(config_file)

        projector_dir = config.get("projector_lib")

        die_if(
            projector_dir is None,
            """\nERROR: The configuration provided is missing a key """
            """configuration property, projector_lib. The value """
            """assigned this property tells whistle where the whistle """
            """code can be found. Please add this to the configuration """
            """file and rerun init-play.\n""",
        )

        print(f"Writing data to {projector_dir}")
        projector_dir = Path(projector_dir)
        projector_dir.mkdir(parents=True, exist_ok=True)
        for module in args.modules:
            module_path = module_options[module]

            print(f"Creating files for module, {module}")
            copy_files(
                str(module_options[module]), projector_dir, context=whistle_context
            )
