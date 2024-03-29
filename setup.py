import os
from setuptools import setup, find_packages, find_namespace_packages

version = {}
with open("wstlr/version.py") as fp:
    exec(fp.read(), version)

root_dir = os.path.dirname(os.path.abspath(__file__))
req_file = os.path.join(root_dir, "requirements.txt")
with open(req_file) as f:
    requirements = f.read().splitlines()

setup(
    name="NCPI-Whistler",
    version=version["__version__"],
    description=f"NCPI Whistler Pipeline {version['__version__']}",
    packages=find_namespace_packages(),
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "play = wstlr.play:exec",
            "buildcm = wstlr.conceptmap:exec",
            "extractjson = wstlr.extractor:exec",
            "bundleup = wstlr.bundle:exec",
            "builddd = wstlr.dd.dd_from_fhir:exec",
            "delfhir = wstlr.purge:exec",
            "inspectjson = wstlr.inspector:exec",
            "buildsrcobs = wstlr.sourcedata.obscomp:exec",
            "buildsrcqr = wstlr.sourcedata.questionnaire:exec",
            "init-play = wstlr.init:exec",
            "igload = wstlr.igload:exec",
            "dd-json-to-csv=wstlr.dd.json_parser:convert_json_to_csv",
        ]
    },
)
