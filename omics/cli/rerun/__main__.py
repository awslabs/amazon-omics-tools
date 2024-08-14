#!/usr/bin/env python3
"""
Command-line tool to rerun an Omics workflow run

Usage: omics-rerun [<runIdOrArn>...]
                   [--start=<date>]
                   [--end=<date>]
                   [--workflow-id=<id>]
                   [--workflow-type=<type>]
                   [--run-id=<id>]
                   [--role-arn=<arn>]
                   [--name=<name>]
                   [--run-group-id=<id>]
                   [--priority=<priority>]
                   [--parameter=<key=value>...]
                   [--storage-capacity=<value>]
                   [--retention-mode=<mode>]
                   [--output-uri=<uri>]
                   [--log-level=<level>]
                   [--tag=<key=value>...]
                   [--count=<value>]
                   [--out=<path>]
                   [--dry-run]
                   [--show]
                   [--help]

Options:
 -s, --start=<date>            Show runs completed after specified date/time (UTC)
 -e, --end=<date>              Show runs completed before specified date/time (UTC)
 --workflow-id=<id>            Override original run parameter
 --workflow-type=<type>        Override original run parameter
 --run-id=<id>                 Override original run parameter
 --role-arn=<arn>              Override original run parameter
 --name=<name>                 Override original run parameter
 --run-group-id=<id>           Override original run parameter
 --priority=<priority>         Override original run parameter
 --parameter=<key=value>...    Override original run parameter
 --storage-capacity=<value>    Override original run parameter
 --retention-mode=<mode>       Override original run parameter
 --output-uri=<uri>            Override original run parameter
 --log-level=<level>           Override original run parameter
 --tag=<key=value>...          Override original run parameter
 -o, --out=<path>              Output to file
 -d, --dry-run                 Show request only
 -h, --help                    Show help text

Examples:
 # Show workflow runs completed on July 1st (UTC time)
 omics-rerun -s 2023-07-01T00:00:00 -e 2023-07-02T00:00:00
 # Rerun specified workflow run, overriding "name" parameter
 omics-rerun 1234567 --name "New run"
 # Dry run specified workflow run
 omics-rerun -d 1234567
"""
import datetime
import json
import os
import re
import sys
import time

import boto3
import botocore
import dateutil
import docopt

exename = os.path.basename(sys.argv[0])


def die(msg):
    """Show error message and terminate"""
    exit(f"{exename}: {msg}")


def stream_to_run(strm):
    """Convert CloudWatch Log stream to workflow run details"""
    m = re.match(r"^manifest/run/(\d+)/[a-f0-9-]+$", strm["logStreamName"])
    if not m:
        return None
    creation_time = datetime.datetime.fromtimestamp(strm["creationTime"] / 1000.0).isoformat(
        timespec="milliseconds"
    )
    return {
        "id": m.group(1),
        "creationTime": creation_time,
        "logStreamName": strm["logStreamName"],
    }


def get_streams(logs, rqst, opts={}):
    """Get matching CloudWatch Log streams"""
    start_time = (
        dateutil.parser.parse(opts["--start"]).timestamp() * 1000.0 if opts.get("--start") else None
    )
    end_time = (
        dateutil.parser.parse(opts["--end"]).timestamp() * 1000.0 if opts.get("--end") else None
    )
    streams = []
    while True:
        try:
            resp = logs.describe_log_streams(**rqst)
            for s in resp["logStreams"]:
                if not stream_to_run(s):
                    pass
                elif start_time and s["creationTime"] < start_time:
                    pass
                elif end_time and s["creationTime"] > end_time:
                    pass
                else:
                    streams.append(s)
            if not resp.get("nextToken") or (not opts and len(streams)):
                break
            rqst["nextToken"] = resp["nextToken"]
        except botocore.exceptions.ClientError as e:
            if "ThrottlingException" in str(e):
                time.sleep(1)
            else:
                raise e
    return streams


def get_runs(logs, runs, opts):
    """Get matching workflow runs"""
    streams = []
    if runs:
        # Get specified runs
        for run in runs:
            run_id = run.split("/")[-1]
            rqst = {
                "logGroupName": "/aws/omics/WorkflowLog",
                "logStreamNamePrefix": f"manifest/run/{run_id}/",
            }
            streams.extend(get_streams(logs, rqst))
    else:
        # Get runs in time range
        rqst = {
            "logGroupName": "/aws/omics/WorkflowLog",
            "logStreamNamePrefix": "manifest/run/",
        }
        streams.extend(get_streams(logs, rqst, opts))
    runs = [stream_to_run(s) for s in streams]
    return sorted(runs, key=lambda x: x["creationTime"])


def get_run_resources(logs, run):
    """Get workflow run/task details"""
    rqst = {
        "logGroupName": "/aws/omics/WorkflowLog",
        "logStreamName": run["logStreamName"],
        "startFromHead": True,
    }
    resources = []
    while True:
        resp = logs.get_log_events(**rqst)
        for evt in resp.get("events", []):
            try:
                resources.append(json.loads(evt["message"]))
            except Exception:
                pass
        token = resp.get("nextForwardToken")
        if not token or token == rqst.get("nextToken"):
            break
        rqst["nextToken"] = token
    return sorted(resources, key=lambda x: x.get("creationTime"))


def get_workflow_type(run):
    """Get workflow type"""
    if not run.get("workflow", None) or len(run["workflow"].split(":")) < 5:
        die(f"Failed to retrieve workflow type from run {run['arn']}")
    return "READY2RUN" if not run["workflow"].split(":")[4] else "PRIVATE"


def start_run_request(run, opts={}):
    """Build StartRun request"""

    def set_param(rqst, key, key0, val=None):
        if not val and opts and key0:
            val = opts[key0]
        if not val:
            val = run.get(key)
        if val:
            rqst[key] = val

    rqst = {}
    if opts.get("--workflow-id"):
        set_param(rqst, "workflowId", "--workflow-id")
    elif opts.get("--run-id"):
        set_param(rqst, "runId", "--run-id")
    elif run.get("run"):
        set_param(rqst, "runId", None, run["run"].split("/")[-1])
    else:
        set_param(rqst, "workflowId", None, run["workflow"].split("/")[-1])

    if opts.get("--workflow-type"):
        set_param(rqst, "workflowType", "--workflow-type")
    else:
        rqst["workflowType"] = get_workflow_type(run)

    set_param(rqst, "roleArn", "--role-arn")
    set_param(rqst, "name", "--name")
    if opts.get("--run-group-id") or run.get("runGroup"):
        group_default = run.get("runGroup", "").split("/")[-1]
        set_param(rqst, "runGroupId", "--run-group-id", group_default)
    set_param(rqst, "priority", "--priority")
    if "priority" in rqst:
        rqst["priority"] = int(rqst["priority"])
    if run.get("parameters"):
        rqst["parameters"] = run["parameters"]
    for p in (opts or {}).get("--parameter", []):
        m = re.match(r"^(\w+)=(\w+)", p)
        if not m:
            die(f"invalid --parameter: {p} (expecting <key>=<value>)")
        if "parameters" not in rqst:
            rqst["parameters"] = {}
        rqst["parameters"][m.group(1)] = m.group(2)
    if rqst["workflowType"] != "READY2RUN":
        set_param(rqst, "storageCapacity", "--storage-capacity")
        if "storageCapacity" in rqst:
            rqst["storageCapacity"] = int(rqst["storageCapacity"])
    set_param(rqst, "retentionMode", "--retention-mode")
    set_param(rqst, "outputUri", "--output-uri")
    set_param(rqst, "logLevel", "--log-level")
    for t in (opts or {}).get("--tag", []):
        m = re.match(r"^(\w+)=(\w+)", t)
        if not m:
            die(f"invalid --tag: {t} (expecting <key>=<value>)")
        if "tags" not in rqst:
            rqst["tags"] = {}
        rqst["tags"][m.group(1)] = m.group(2)
    return rqst


if __name__ == "__main__":
    opts = docopt.docopt(__doc__)

    try:
        logs = boto3.client("logs")
    except Exception as e:
        die(f"CloudWatch Logs client create failed: {e}")
    runs = get_runs(logs, opts["<runIdOrArn>"], opts)
    if not runs:
        die("no matching workflow runs")

    out = open(opts["--out"], "w") if opts["--out"] else sys.stdout
    if len(runs) != 1 or not opts["<runIdOrArn>"]:
        # Show available runs
        out.write("Runs:\n")
        for r in runs:
            out.write(f"{r['id']} ({r['creationTime']})\n")
    else:
        resources = get_run_resources(logs, runs[0])
        run = [r for r in resources if r["arn"].endswith(f"run/{runs[0]['id']}")]
        run = run[0] if run else None
        if not resources:
            die("no workflow run resources")
        elif not run:
            die("no workflow run details")
        elif not run.get("workflow") and not run.get("run"):
            die("no workflow or run IDs")
        else:
            # Rerun specified run
            rqst0 = start_run_request(run)
            rqst = start_run_request(run, opts)
            if rqst != rqst0:
                out.write(f"Original request:\n{json.dumps(rqst0, indent=2)}\n")
            out.write(f"StartRun request:\n{json.dumps(rqst, indent=2)}\n")
            if not opts["--dry-run"]:
                try:
                    omics = boto3.client("omics")
                    resp = omics.start_run(**rqst)
                except Exception as e:
                    die(f"StartRun failed: {e}")
                del resp["ResponseMetadata"]  # type: ignore
                out.write(f"StartRun response:\n{json.dumps(resp, indent=2)}\n")

    if opts["--out"]:
        out.close()
        sys.stderr.write(f"{exename}: wrote {opts['--out']}\n")
