from collections import defaultdict
from rich import print


class ModuleSummary:
    def __init__(self, resource_types=None):
        # Capture details specific to a subset of resource types (if specified)
        self.resource_types = resource_types

        # Module-name => ResourceType => count
        self.module_summary = defaultdict(lambda: defaultdict(int))
        self.resource_summary = defaultdict(int)

    def summary(self, group_name, resource):
        resourceType = resource["resourceType"]
        if self.resource_types is None or resourceType in self.resource_types:
            self.module_summary[group_name][resourceType] += 1
            self.resource_summary[resourceType] += 1

    def print_summary(self, study_id):
        print(f"\nModule Summary [green]({study_id})[/green]")
        print(
            "Module Name                      Resource Type            #         % of Total"
        )
        print(
            "-------------------------------  ------------------------ --------- ----------"
        )
        for modulename in sorted(self.module_summary.keys()):
            for resourcetype in sorted(self.module_summary[modulename].keys()):
                observed = self.module_summary[modulename][resourcetype]
                total = self.resource_summary[resourcetype]
                perc = f"{(100.0 * observed)/total:4.2f}"  # .rstrip("0").rstrip(".")
                print(
                    f"{modulename:<32} {resourcetype:<24} {self.module_summary[modulename][resourcetype]:<9} {perc:>7}"
                )
