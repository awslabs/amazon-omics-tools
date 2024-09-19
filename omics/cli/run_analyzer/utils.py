import re

ENGINES = ["WDL", "CWL", "Nextflow"]

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
    elif engine == "Nextflow":
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


def omics_instance_weight(instance: str) -> int:
    """Compute a numeric weight for an instance to be used in sorting or finding a max or min"""
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
    families = {"c": 2, "m": 4, "r": 8, "g4dn": 16, "g5": 16}
    # remove the "omics." from the string
    instance.replace("omics.", "")
    # split the instance into family and size
    parts = instance.split(".")
    fam = parts[0]
    size = parts[1] if len(parts) > 1 else ""

    ccount = sizes[size]
    weight = ccount * families[fam]
    return weight
