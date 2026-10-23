"""
 * Basic functionality around loading fhir hosts file and autogeneration if
 * the file doesn't exist
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, TextIO

from ncpi_fhir_client import fhir_auth
from yaml import safe_load

_default_hosts_file = "fhir_hosts"

def example_config(writer: TextIO, auth_type: str | None = None) -> None:
    """Returns a block of text containing one or all possible auth modules example configurations"""

    modules = fhir_auth.get_modules()
    print(
        f"""# Example Hosts Configuration.
# 
# This is a basic yaml file (yaml.org) where each root level tag represents a 
# system "name" and it's children's keys represent key/values to assign to a 
# host configuration which includes the authentication details.
#
# All host entries should have the following key/values:
# host_desc             - This is just a short description which can be used
#                         for log names or whatnot
# target_service_url    - This is the URL associated with the actual API 
# auth_type             - This is the module name for the authentication used
#                         by the specified host
#
# Please note that there can be multiple hosts that use the same authentication
# mechanism. Users must ensure that each host has a unique "key" """,
        file=writer,
    )
    for key in modules.keys():
        if auth_type is None or auth_type == key:
            other_entries = {
                "host_desc": f"Example {key}",
                "target_service_url": "https://example.fhir.server/R4/fhir",
            }

            modules[key].example_config(writer, other_entries)

def load_hosts_file(filename: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    if filename is None:
        filename = _default_hosts_file
    host_config_filename = Path(filename)

    if not host_config_filename.is_file() or host_config_filename.stat().st_size == 0:
        example_config(sys.stdout)
        sys.stderr.write(
            f"""
A valid host configuration file, fhir_hosts, must exist in cwd and was not 
found. Example configuration has been written to stout providing examples 
for each of the auth types currently supported.\n"""
        )
        sys.exit(1)

    host_config = safe_load(host_config_filename.open("rt"))

    return host_config