from pathlib import Path
import zipfile
import requests
from tempfile import TemporaryFile
import json

import sys




# Return a list of valid JSON objects ready for loading
def load_resources(config):
        resources = {}
        f = None

        ig_source = ""

        if 'url' in config:
            # Grab the definition zip file from the IG website
            url = f"{config['url']}/definitions.json.zip"
            ig_source = url
            response = requests.get(url)

            # Open it up using a Temporary File
            f = TemporaryFile()
            f.write(response.content)
            f.seek(0)
        elif 'path' in config:
            # If we have a path, then we just open the file as usual
            ig_source = config['path']
            f = (Path(config['path']) / "output/definitions.json.zip").open('rb')
        else:
            sys.stderr("ERROR: Each module MUST contain either a 'path' or a 'url' pointing to a valid IG produced by HL7s publisher.")
            sys.exit(1)

        # These are plain zip files, so just open it up using python's Zipfile
        zipped = zipfile.ZipFile(f)

        # Iterate over each of the entries 
        for f in zipped.infolist():
            filename = f.filename

            resources[filename] = json.loads(zipped.read(filename).decode())

        print(f"{len(resources)} resources found at: {ig_source}")