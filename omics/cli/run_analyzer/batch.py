import sys

from . import __main__ as main


hdrs = [
            "type",
            "name",
            "count",
            "meanRunningSeconds",
            "maximumRunningSeconds",
            "stdDevRunningSeconds",
            "maximumPeakCpuUtilization",
            "maximumPeakMemoryUtilization",
            "maxGpusReserved",
            "recommendedCpus",
            "recommendedMemoryGiB",
            "recommendOmicsInstanceType",
            "estimatedUSDForMaximumRunningSeconds",
            "estimatedUSDForMeanRunningSeconds",
            "storageMaximumGiB",
            "meanStorageMaximumGiB",
            "stdDevStorageMaximumGiB"
        ]

def aggregate_and_print(resources_list, pricing, headroom=0.0, out=sys.stdout):
    """Aggregate resources and print to output"""
    for resources in resources_list:        
        # filter resources to remove anything where res["type"] is not "run"
        resources = [r for r in resources if r["type"] == "run"]
        for res in resources:
            main.add_metrics(res, resources, pricing, headroom)

        # if there are scattter resources from the run with a common name prefix then aggregate
        names = [r["name"] for r in resources]
        names = list(set(names))
        for name in names:
            _aggregate_resources(resources, name)
