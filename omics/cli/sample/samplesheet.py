#!/usr/bin/env python3
"""
Command-line tool to create a sample sheet

Usage: omics-samples [<sequenceStoreId>]
                   [--start=<date>]
                   [--end=<date>]
                   [--out=<path>]
                   [--help]

Options:
 -s, --start=<date>            Show runs completed after specified date/time (UTC)
 -e, --end=<date>              Show runs completed before specified date/time (UTC)
 -h, --help                    Show help text

Examples:
 # Create a sample sheet with samples created after the specified date
 omics-samples 1234567890 -s 2023-07-01
"""
import boto3
import docopt
from dateutil import parser

session = boto3.Session()
omics = session.client("omics")
omics_client = boto3.client("omics")


def get_samples(sqnid, filter):
    samples = []
    paginator = omics_client.get_paginator("list_read_sets")

    params = {"sequenceStoreId": sqnid, "filter": filter}

    for page in paginator.paginate(**params):
        read_sets = page.get("readSets", [])
        for read_set in read_sets:
            data = omics_client.get_read_set_metadata(id=read_set["id"], sequenceStoreId=sqnid)
            sample = data["sampleId"]
            uri = data["files"]["source1"]["s3Access"]["s3Uri"]
            samples.append(f"{sample},{uri}")
    return samples


def get_filter(cli_opts) -> dict:
    filter = {}
    if opts["--start"]:
        filter["createdAfter"] = parser.parse(opts["--start"])
    if opts["--end"]:
        filter["createdBefore"] = parser.parse(opts["--end"])
    return filter


def write_samples(samples, outpath):
    with open(outpath, "w") as out:
        for sample in samples:
            out.write(sample + "\n")


def main():
    sequence_store = opts["<sequenceStoreId>"]
    filter = get_filter(opts)
    samples = get_samples(sequence_store, filter)
    if opts["--out"]:
        write_samples(samples, opts["--out"])
    else:
        for sample in samples:
            print(sample)


if __name__ == "__main__":
    opts = docopt.docopt(__doc__)
    main()
