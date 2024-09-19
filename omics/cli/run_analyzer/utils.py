import re

ENGINES = set(["WDL", "CWL", "NEXTFLOW"])

_wdl_task_regex = r"^([^-]+)(-\d+-\d+.*)?$"
_nextflow_task_regex = r"^(.+)(\s\(.+\))$"
_cwl_task_regex = r"^(^\D+)(_\d+)?$"


def get_engine(workflow_arn, session) -> str:
    """Get the engine name for the workflow_arn"""
    omics = session.client("omics")
    id = workflow_arn.split("/")[-1]
    return omics.get_workflow(id)["engine"]


def task_base_name(name: str, engine: str) -> str:
    """Find the base name of the task assuming the naming conventions used by the engine"""
    # WDL
    if engine == "WDL":
        m = re.match(_wdl_task_regex, name)
        if m:
            return m.group(1)
    # Nextflow
    elif engine == "NEXTFLOW":
        m = re.match(_nextflow_task_regex, name)
        if m:
            return m.group(1)
    # CWL
    elif engine == "CWL":
        m = re.match(_cwl_task_regex, name)
        if m:
            return m.group(1)
    else:
        raise ValueError(f"Unsupported engine: {engine}")
    return name


_sizes = {
    "": 2,
    "xlarge": 4,
    "2xlarge": 8,
    "4xlarge": 16,
    "8xlarge": 32,
    "12xlarge": 48,
    "16xlarge": 64,
    "24xlarge": 96,
}
_families = {"c": 2, "m": 4, "r": 8, "g4dn": 16, "g5": 16}


def omics_instance_weight(instance: str) -> int:
    """Compute a numeric weight for an instance to be used in sorting or finding a max or min"""
    print(instance)
    # remove the "omics." from the string
    instance = instance.replace("omics.", "")
    # split the instance into family and size
    parts = instance.split(".")
    print(parts)
    fam = parts[0]
    size = parts[1]

    ccount = _sizes[size]
    weight = ccount * _families[fam]
    return weight
