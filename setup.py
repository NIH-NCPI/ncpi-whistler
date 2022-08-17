import os
from setuptools import setup, find_packages

from wstlr import __version__

root_dir = os.path.dirname(os.path.abspath(__file__))
req_file = os.path.join(root_dir, "requirements.txt")
with open(req_file) as f:
    requirements = f.read().splitlines()

setup(
    name="NCPI-Whistler",
    version=__version__,
    description=f"NCPI Whistler Pipeline {__version__}",
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'play = wstlr.play:exec',
            'buildcm = wstlr.conceptmap:exec',
            'extractjson = wstlr.extractor:exec',
            'bundleup = wstlr.bundle:exec',
            'builddd = wstlr.dd:exec',
            'delfhir = wstlr.purge:exec',
            'inspectjson = wstlr.inspector:exec',
            'buildsrcobs = wstlr.sourcedata.obscomp:exec',
            'buildsrcqr = wstlr.sourcedata.questionnaire:exec'
        ]
    }
)
