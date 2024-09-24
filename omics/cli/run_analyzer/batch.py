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


def aggregate_and_print(run_resources_list: list[list[dict]], pricing, engine, headroom=0.0, out=sys.stdout):
    """Aggregate resources and print to output"""
    if engine not in utils.ENGINES:
        raise ValueError(
            f"Invalid engine for use in batch aggregation: {engine}. Must be one of {utils.ENGINES}"
        )

    task_names = set()
    for run_resources in run_resources_list:
        # filter run resources to remove anything where "type" is not "run"
        resources = [r for r in run_resources if r["type"] == "run"] 
        for res in resources:
            main.add_metrics(res, resources, pricing, headroom)
            task_names.add(utils.task_base_name(res["name"], engine))

    # print hdrs
    print(",".join(hdrs), file=out)

    task_names = sorted(task_names)
    for task_name in task_names:
        _aggregate_resources(run_resources_list, task_name, engine, out)


def _aggregate_resources(run_resources_list: list[list[dict]], task_name: str, engine: str, out):
    """Aggregate resources with the same name"""
    for run_resources in run_resources_list:
        # find resources in run_resources that have a name matching the task_name
        run_resources = [r for r in run_resources if utils.task_base_name(r["name"], engine) == task_name]

    # for each header key, perform the aggregation
    aggregate = {}
    for k in hdrs:
        if k == "type":
            aggregate[k] = "run"
        elif k == "name":
            aggregate[k] = task_name
        elif k == "count":
            aggregate[k] = _do_aggregation(run_resources_list, k, "count")
        elif k.startswith("mean"):
            # resource key is k with "mean" removed and the first char to lowercase
            rk = k.replace("mean", "")[0].lower() + k.replace("mean", "")[1:]
            aggregate[k] = _do_aggregation(run_resources_list, rk, "mean")
        elif k.startswith("stdDev"):
            rk = k.replace("stdDev", "")[0].lower() + k.replace("stdDev", "")[1:]
            aggregate[k] = _do_aggregation(run_resources_list, rk, "stdDev")
        elif k.startswith("maximum"):
            rk = k.replace("maximum", "")[0].lower() + k.replace("maximum", "")[1:]
            aggregate[k] = _do_aggregation(run_resources_list, rk, "maximum")
        elif k in ["recommendedCpus", "recommendedMemoryGiB"]:
            aggregate[k] = _do_aggregation(run_resources_list, k, "maximum")
        elif k in ["recommendOmicsInstanceType"]:
            aggregate[k] = _do_aggregation(run_resources_list, "omicsInstanceTypeMinimum", "maximum")
        else:
            raise ValueError(f"Unhandled aggregation for key: {k}")

    print(",".join([str(aggregate.get(h, "")) for h in hdrs]), file=out)


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
