
from pathlib import Path
import zipfile
import requests
from tempfile import TemporaryFile
import json

import sys

def load_resources(config):
    resources = {}
    
    for filename in config['resources']:
        resource = None
        if filename.lower()[:4] == "http":
            response = requests.get(filename)
            response.raise_for_status()
            resource = response.json()
        else:
            f = Path(filename).open('rt')
            resource = json.load(f)

        if resource is not None:
            resources[filename] = resource

    return resources