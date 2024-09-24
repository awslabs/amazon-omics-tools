import statistics
import sys

from . import __main__ as main
from . import utils

hdrs = [
    "type",
    "name",
    "count",
    "meanRunningSeconds",
    "maximumRunningSeconds",
    "stdDevRunningSeconds",
    "maximumCpuUtilizationRatio",
    "maximumMemoryUtilizationRatio",
    "maximumGpusReserved",
    "recommendedCpus",
    "recommendedMemoryGiB",
    "recommendOmicsInstanceType",
    "maximumEstimatedUSD",
    "meanEstimatedUSD",
]


def aggregate_and_print(resources_list, pricing, engine, headroom=0.0, out=sys.stdout):
    """Aggregate resources and print to output"""
    if engine not in utils.ENGINES:
        raise ValueError(
            f"Invalid engine for use in batch aggregation: {engine}. Must be one of {utils.ENGINES}"
        )

    for resources in resources_list:
        # filter resources to remove anything where res["type"] is not "run"
        resources = [r for r in resources if r["type"] == "run"]
        for res in resources:
            main.add_metrics(res, resources, pricing, headroom)

    # if there are resources from the runs with a common name or prefix then aggregate
    names = [utils.task_base_name(r["name"]) for r in resources]
    names = list(set(names))
    names.sort()

    # print hdrs
    print(", ".join(hdrs), file=out)

    for name in names:
        _aggregate_resources(resources, name, out)


def _aggregate_resources(resources, name, engine, out):
    """Aggregate resources with the same name"""
    filtered = [r for r in resources if utils.task_base_name(r["name"], engine) == name]
    if filtered:
        res = filtered[0]
        for k in hdrs:
            if k == "type":
                continue
            elif k == "name":
                res[k] = name
            elif k == "count":
                res[k] = _do_aggregation(filtered, k, "count")
            elif k.startswith("mean"):
                # resource key is k with "mean" removed and the first char to lowercase
                rk = k.replace("mean", "")[0].lower() + k.replace("mean", "")[1:]
                res[k] = _do_aggregation(filtered, rk, "mean")
            elif k.startswith("stdDev"):
                rk = k.replace("stdDev", "")[0].lower() + k.replace("stdDev", "")[1:]
                res[k] = _do_aggregation(filtered, rk, "stdDev")
            elif k.startswith("maximum"):
                rk = k.replace("maximum", "")[0].lower() + k.replace("maximum", "")[1:]
                res[k] = _do_aggregation(filtered, rk, "maximum")
            elif k in ["recommendedCpus", "recommendedMemoryGiB"]:
                res[k] = _do_aggregation(filtered, k, "maximum")
            elif k in ["recommendOmicsInstanceType"]:
               res[k] = _do_aggregation(filtered, "omicsInstanceTypeMinimum", "maximum")
            else:
                raise ValueError(f"Unhandled aggregation for key: {k}")

    print(",".join([str(res.get(h, "")) for h in hdrs]), file=out)


def _do_aggregation(resources: list, resource_key: str, operation: str):
    if operation == "count":
        return len(resources)
    elif operation == "sum":
        return sum([r[resource_key] for r in resources])
    elif operation == "maximum":
        if resource_key == "omicsInstanceTypeMinimum":
            # special case for instance types
            instances=[]
            for r in resources: instances.append(r[resource_key])
            return max(instances, key=lambda x: utils.omics_instance_weight(x))
        return max([r[resource_key] for r in resources])
    elif operation == "mean":
        return round(statistics.mean([r[resource_key] for r in resources]), 2)
    elif operation == "stdDev":
        return round(statistics.stdev([r[resource_key] for r in resources]), 2)
    else:
        raise ValueError(f"Invalid aggregation operation: {operation}")
