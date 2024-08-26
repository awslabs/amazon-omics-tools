#!/usr/bin/env python3
"""
Generate statistics for a completed HealthOmics workflow run

Usage: omics-run-analyzer [<runId>...]
                          [--profile=<profile>]
                          [--region=<region>]
                          [--time=<interval>]
                          [--show]
                          [--timeline]
                          [--file=<path>]
                          [--out=<path>]
                          [--plot=<directory>]
                          [--help]

Options:
 -p, --profile=<profile>  AWS profile
 -r, --region=<region>    AWS region
 -t, --time=<interval>    Select runs over a time interval [default: 1day]
 -s, --show               Show run resources with no post-processing (JSON)
 -T, --timeline           Show workflow run timeline
 -f, --file=<path>        Load input from file
 -o, --out=<path>         Write output to file
 -P, --plot=<directory>   Plot a run timeline to a directory
 -h, --help               Show help text

Examples:
 # Show workflow runs that were running in the last 5 days
 # (supported time units include minutes, hours, days, weeks, or years)
 omics-run-analyzer --time=5days
 # Retrieve and analyze a specific workflow run by ID
 omics-run-analyzer 1234567 -o run-1234567.csv
 # Retrieve and analyze a specific workflow run by ID and UUID
 omics-run-analyzer 2345678:12345678-1234-5678-9012-123456789012
 # Output workflow run and tasks in JSON format
 omics-run-analyzer 1234567 -s -o run-1234567.json
"""
import csv
import datetime
import json
import os
import re
import sys

import boto3
import dateutil
import dateutil.utils
import docopt
from bokeh.plotting import output_file

from . import timeline  # type: ignore

exename = os.path.basename(sys.argv[0])
OMICS_LOG_GROUP = "/aws/omics/WorkflowLog"
OMICS_SERVICE_CODE = "AmazonOmics"
PRICING_AWS_REGION = "us-east-1"  # Pricing service endpoint
SECS_PER_HOUR = 3600.0
STORAGE_TYPE_DYNAMIC_RUN_STORAGE = "DYNAMIC"
STORAGE_TYPE_STATIC_RUN_STORAGE = "STATIC"
PRICE_RESOURCE_TYPE_DYNAMIC_RUN_STORAGE = "Dynamic Run Storage"
PRICE_RESOURCE_TYPE_STATIC_RUN_STORAGE = "Run Storage"


def die(msg):
    """Show error message and terminate"""
    exit(f"{exename}: {msg}")


def parse_time_str(s, utc=True):
    """Parse time string"""
    tz = datetime.timezone.utc
    return dateutil.parser.parse(s).replace(tzinfo=tz) if s else None


def parse_time_delta(s):
    """Parse time delta string"""
    m = re.match(r"(\d+)\s*(m|min|minutes?|h|hours?|d|days?|w|weeks?|y|years?)$", s)
    if not m:
        die("unrecognized time interval format '{}'".format(s))
    secs = {"m": 60, "h": 3600, "d": 86400, "w": 604800, "y": 220752000}
    delta = int(m.group(1)) * secs[m.group(2)[0]]
    return datetime.timedelta(seconds=delta)


def get_static_storage_gib(capacity=None):
    """Return filesystem size in GiB"""
    omics_storage_min = 1200  # Minimum size
    omics_storage_inc = 2400  # Size increment (2400, 4800, 7200, ...)
    if not capacity or capacity <= omics_storage_min:
        return omics_storage_min
    capacity = (capacity + omics_storage_inc - 1) / omics_storage_inc
    return int(capacity) * omics_storage_inc


def get_instance(cpus, mem):
    """Return a tuple of smallest matching instance type (str), cpus in that type (int), GiB memory of that type (int)"""
    sizes = {
        "": 2,
        "x": 4,
        "2x": 8,
        "4x": 16,
        "8x": 32,
        "12x": 48,
        "16x": 64,
        "24x": 96,
    }
    families = {"c": 2, "m": 4, "r": 8}
    for size in sorted(sizes, key=lambda x: sizes[x]):
        ccount = sizes[size]
        if ccount < cpus:
            continue
        for fam in sorted(families, key=lambda x: families[x]):
            mcount = ccount * families[fam]
            if mcount < mem:
                continue
            return (f"omics.{fam}.{size}large", ccount, mcount)
    return ""


def get_pricing(pricing, resource, region, hours):
    key = f"{resource}:{region}"
    price = get_pricing.pricing.get(key)
    if price:
        return price * hours
    elif not pricing:
        return None
    filters = [
        {"Type": "TERM_MATCH", "Field": "resourceType", "Value": resource},
        {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
    ]
    rqst = {"ServiceCode": OMICS_SERVICE_CODE, "Filters": filters}
    for page in pricing.get_paginator("get_products").paginate(**rqst):
        for item in page["PriceList"]:
            entry = json.loads(item)
            price = entry.get("terms", {}).get("OnDemand", {})
            price = next(iter(price.values()), {}).get("priceDimensions", {})
            price = next(iter(price.values()), {}).get("pricePerUnit", {})
            price = price.get("USD")
            if price is None:
                continue
            price = float(price)
            get_pricing.pricing[key] = price
            return price * hours
    return None


get_pricing.pricing = {}


def stream_to_run(strm):
    """Convert CloudWatch Log stream to workflow run details"""
    m = re.match(r"^manifest/run/(\d+)/([a-f0-9-]+)$", strm["logStreamName"])
    if not m:
        return None
    strm["id"] = m.group(1)
    strm["uuid"] = m.group(2)
    return strm


def get_streams(logs, rqst, start_time=None):
    """Get matching CloudWatch Log streams"""
    streams = []
    for page in logs.get_paginator("describe_log_streams").paginate(**rqst):
        done = False
        for strm in page["logStreams"]:
            if start_time and strm["lastEventTimestamp"] < start_time:
                done = True
            elif stream_to_run(strm):
                streams.append(strm)
                if (len(streams) % 100) == 0:
                    sys.stderr.write(f"{exename}: found {len(streams)} workflow runs\n")
                if not start_time:
                    done = True
        if done:
            break
    return streams


def get_runs(logs, runs, opts):
    """Get matching workflow runs"""
    streams = []
    if runs:
        # Get specified runs
        for run in runs:
            run = re.split(r"[:/]", run)
            if re.match(r"[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$", run[-1]):
                prefix = f"manifest/run/{run[-2]}/{run[-1]}"
            else:
                prefix = f"manifest/run/{run[-1]}/"
            rqst = {
                "logGroupName": OMICS_LOG_GROUP,
                "logStreamNamePrefix": prefix,
            }
            streams.extend(get_streams(logs, rqst))
    else:
        # Get runs in time range
        start_time = datetime.datetime.now() - parse_time_delta(opts["--time"])
        start_time = start_time.timestamp() * 1000.0
        rqst = {
            "logGroupName": OMICS_LOG_GROUP,
            "orderBy": "LastEventTime",
            "descending": True,
        }
        streams.extend(get_streams(logs, rqst, start_time))
    runs = [stream_to_run(s) for s in streams]
    return sorted(runs, key=lambda x: x["creationTime"])


def get_run_resources(logs, run):
    """Get workflow run/task details"""
    rqst = {
        "logGroupName": OMICS_LOG_GROUP,
        "logStreamName": run["logStreamName"],
        "startFromHead": True,
    }
    resources = []
    done = False
    while not done:
        resp = logs.get_log_events(**rqst)
        for evt in resp.get("events", []):
            try:
                resources.append(json.loads(evt["message"]))
            except Exception:
                pass
            if evt["timestamp"] >= run["lastEventTimestamp"]:
                done = True
        token = resp.get("nextForwardToken")
        if not token or token == rqst.get("nextToken"):
            done = True
        rqst["nextToken"] = token
    return sorted(resources, key=lambda x: x.get("creationTime"))


def add_run_util(run, tasks):
    """Add run metrics computed from task metrics"""
    events = []
    stop1 = None
    stops = []
    for idx, task in enumerate(tasks):
        start = parse_time_str(task.get("startTime"))
        if start:
            events.append({"time": start, "event": "start", "index": idx})
        stop = parse_time_str(task.get("stopTime"))
        if stop:
            events.append({"time": stop, "event": "stop", "index": idx})
            if not stop1 or stop > stop1:
                stop1 = stop
        else:
            stops.append(idx)
    for idx in stops:
        events.append({"time": stop1, "event": "stop", "index": idx})
    events.sort(key=lambda x: x["time"])

    metric_names = [
        "cpusReserved",
        "cpusMaximum",
        "cpusAverage",
        "gpusReserved",
        "memoryReservedGiB",
        "memoryMaximumGiB",
        "memoryAverageGiB",
    ]
    metrics = run.get("metrics", {})
    run["metrics"] = metrics

    active = []
    t0 = None
    time = 0
    for evt in events:
        t1 = evt["time"]
        if t0:
            secs = (t1 - t0).total_seconds()
            time += secs
            for name in metric_names:
                task_metrics = tasks[idx].get("metrics", {})
                mvalues = [task_metrics.get(name) for idx in active]
                mvalues = [v for v in mvalues if v is not None]
                if not mvalues:
                    continue
                total = sum(mvalues)
                if "Average" in name:
                    metrics[name] = metrics.get(name, 0) + total * secs
                else:
                    metrics[name] = max(metrics.get(name, total), total)
        t0 = t1
        if evt["event"] == "start":
            active.append(evt["index"])
        elif evt["index"] in active:
            active.remove(evt["index"])

    for name in metric_names:
        if name in metrics and "Average" in name:
            metrics[name] /= time


def add_metrics(res, resources, pricing):
    """Add run/task metrics"""
    arn = re.split(r"[:/]", res["arn"])
    rtype = arn[-2]
    region = arn[3]
    res["type"] = rtype

    metrics = res.get("metrics", {})
    # if a resource has no metrics body then we can skip the rest
    if res.get("metrics") is None:
        return

    if rtype == "run":
        add_run_util(res, resources[1:])

    time1 = parse_time_str(res.get("startTime"))
    time2 = parse_time_str(res.get("stopTime"))
    running = 0
    if time1 and time2:
        running = (time2 - time1).total_seconds()
        metrics["runningSeconds"] = running

    cpus_res = metrics.get("cpusReserved")
    cpus_max = metrics.get("cpusMaximum")
    if cpus_res and cpus_max:
        metrics["cpuUtilizationRatio"] = float(cpus_max) / float(cpus_res)
    gpus_res = metrics.get("gpusReserved")
    mem_res = metrics.get("memoryReservedGiB")
    mem_max = metrics.get("memoryMaximumGiB")
    if mem_res and mem_max:
        metrics["memoryUtilizationRatio"] = float(mem_max) / float(mem_res)
    store_res = metrics.get("storageReservedGiB", 0.0)
    store_max = metrics.get("storageMaximumGiB", 0.0)
    store_avg = metrics.get("storageAverageGiB", 0.0)
    if store_res and store_max:
        metrics["storageUtilizationRatio"] = float(store_max) / float(store_res)

    storage_type = res.get("storageType")

    if rtype == "run":
        # Get capacity requested (static), capacity max. used (dynamic) and
        # charged storage (the requested capacity for static or average used for dynamic)
        if storage_type == STORAGE_TYPE_STATIC_RUN_STORAGE:
            price_resource_type = PRICE_RESOURCE_TYPE_STATIC_RUN_STORAGE
            capacity = get_static_storage_gib(res.get("storageCapacity"))
            charged = capacity
        elif storage_type == STORAGE_TYPE_DYNAMIC_RUN_STORAGE:
            price_resource_type = PRICE_RESOURCE_TYPE_DYNAMIC_RUN_STORAGE
            capacity = store_max
            charged = store_avg

        # Get price for actually used storage (approx. for dynamic storage)
        gib_hrs = charged * running / SECS_PER_HOUR
        price = get_pricing(pricing, price_resource_type, region, gib_hrs)
        if price:
            metrics["estimatedUSD"] = price

        # Get price for optimal static storage
        if store_max:
            capacity = get_static_storage_gib(store_max)
        gib_hrs = capacity * running / SECS_PER_HOUR
        price = get_pricing(pricing, PRICE_RESOURCE_TYPE_STATIC_RUN_STORAGE, region, gib_hrs)
        if price:
            metrics["minimumUSD"] = price

    elif "instanceType" in res:
        runningForInstanceCost = max(60, running)
        itype = res["instanceType"]
        metrics["omicsInstanceTypeReserved"] = itype
        price = get_pricing(pricing, itype, region, runningForInstanceCost / SECS_PER_HOUR)
        if price:
            metrics["estimatedUSD"] = price
        if cpus_max and mem_max and not gpus_res:
            (itype, cpus, mem) = get_instance(cpus_max, mem_max)
            metrics["omicsInstanceTypeMinimum"] = itype
            metrics["recommendedCpus"] = cpus
            metrics["recommendedMemoryGiB"] = mem
        else:
            metrics["omicsInstanceTypeMinimum"] = itype
            metrics["recommendedCpus"] = cpus_res
            metrics["recommendedMemoryGiB"] = mem_res
        price = get_pricing(pricing, itype, region, runningForInstanceCost / SECS_PER_HOUR)
        if price:
            metrics["minimumUSD"] = price


def get_timeline_event(res, resources):
    """Convert resource to timeline event"""
    arn = re.split(r"[:/]", res["arn"])
    time0 = parse_time_str(resources[0].get("creationTime"))
    time1 = parse_time_str(res.get("creationTime"))
    time2 = parse_time_str(res.get("startTime"))
    time3 = parse_time_str(res.get("stopTime"))
    attrs = ["name", "cpus", "gpus", "memory"]
    attrs = [f"{a}={res[a]}" for a in attrs if res.get(a)]
    resource = f"{arn[-2]}/{arn[-1]}"
    if attrs:
        resource += f" ({','.join(attrs)})"
    return {
        "resource": resource,
        "pending": (time1 - time0).total_seconds(),
        "starting": (time2 - time1).total_seconds(),
        "running": (time3 - time2).total_seconds(),
    }


if __name__ == "__main__":
    # Parse command-line options
    opts = docopt.docopt(__doc__)

    try:
        session = boto3.Session(profile_name=opts["--profile"], region_name=opts["--region"])
        pricing = session.client("pricing", region_name=PRICING_AWS_REGION)
        pricing.describe_services(ServiceCode=OMICS_SERVICE_CODE)
    except Exception as e:
        die(e)

    # Retrieve workflow runs & tasks
    runs = []
    resources = []
    if opts["--file"]:
        with open(opts["--file"]) as f:
            resources = json.load(f)
    else:
        try:
            logs = session.client("logs")
            runs = get_runs(logs, opts["<runId>"], opts)
        except Exception as e:
            die(e)
        if not runs:
            die("no matching workflow runs")

        if len(runs) == 1 and opts["<runId>"]:
            resources = get_run_resources(logs, runs[0])
            if not resources:
                die("no workflow run resources")

    # Display output
    with open(opts["--out"] or sys.stdout.fileno(), "w") as out:
        if not resources:
            # Show available runs
            out.write("Workflow run IDs (<completionTime> <UUID>):\n")
            for r in runs:
                time0 = r["creationTime"] / 1000.0
                time0 = datetime.datetime.fromtimestamp(time0)
                time0 = time0.isoformat(timespec="seconds")
                out.write(f"{r['id']} ({time0} {r['uuid']})\n")
        elif opts["--show"]:
            # Show run resources
            out.write(json.dumps(resources, indent=2) + "\n")
        elif opts["--timeline"]:
            # Show run timeline
            hdrs = ["resource", "pending", "starting", "running"]
            writer = csv.writer(out, lineterminator="\n")
            writer.writerow(hdrs)
            for res in resources:
                event = get_timeline_event(res, resources)
                row = [event.get(h, "") for h in hdrs]
                writer.writerow(row)
        else:
            # Show run statistics
            def tocsv(val):
                if val is None:
                    return ""
                return f"{val:f}" if type(val) is float else str(val)

            hdrs = [
                "arn",
                "type",
                "name",
                "startTime",
                "stopTime",
                "runningSeconds",
                "cpus",
                "gpus",
                "memory",
                "omicsInstanceTypeReserved",
                "omicsInstanceTypeMinimum",
                "recommendedCpus",
                "recommendedMemoryGiB",
                "estimatedUSD",
                "minimumUSD",
                "cpuUtilizationRatio",
                "memoryUtilizationRatio",
                "storageUtilizationRatio",
                "cpusReserved",
                "cpusMaximum",
                "cpusAverage",
                "gpusReserved",
                "memoryReservedGiB",
                "memoryMaximumGiB",
                "memoryAverageGiB",
                "storageReservedGiB",
                "storageMaximumGiB",
                "storageAverageGiB",
            ]

            # Rename these headers for consistency
            hrdrs_map = {
                "cpus": "cpusRequested",
                "gpus": "gpusRequested",
                "memory": "memoryRequestedGiB",
            }

            formatted_headers = [hrdrs_map.get(h, h) for h in hdrs]

            writer = csv.writer(out, lineterminator="\n")
            writer.writerow(formatted_headers)
            for res in resources:
                add_metrics(res, resources, pricing)
                metrics = res.get("metrics", {})
                row = [tocsv(metrics.get(h, res.get(h))) for h in hdrs]
                writer.writerow(row)
        if opts["--out"]:
            sys.stderr.write(f"{exename}: wrote {opts['--out']}\n")
    if opts["--plot"]:
        if len(resources) < 1:
            die("no resources to plot")

        run = {}
        for res in resources:
            rtype = re.split(r"[:/]", res["arn"])[-2]
            if rtype == "run":
                run = res
                resources.remove(res)  # we don't want the run in the data to plot
                break

        start = datetime.datetime.strptime(run["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        stop = datetime.datetime.strptime(run["stopTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        run_duration_hrs = (stop - start).total_seconds() / 3600

        runid = run["arn"].split("/")[-1]
        output_file_basename = f"{runid}_timeline"

        # open or create the plot directory
        plot_dir = opts["--plot"]
        if not os.path.isdir(plot_dir):
            os.makedirs(plot_dir)
        output_file(
            filename=os.path.join(plot_dir, f"{output_file_basename}.html"), title=runid, mode="cdn"
        )
        title = f"arn: {run['arn']}, name: {run.get('name')}"

        timeline.plot_timeline(resources, title=title, max_duration_hrs=run_duration_hrs)
