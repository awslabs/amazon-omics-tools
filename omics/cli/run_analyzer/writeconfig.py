import textwrap


def create_config(engine, task_resources, filename):
    """Create a config file based on recommended CPU and Memory values"""
    if engine == "NEXTFLOW":
        with open(filename, "w") as out:
            for task in task_resources:
                task_string = textwrap.dedent(
                    f"""
                withName: {task} {{
                    cpu = {task_resources[task]['cpus']}
                    memory = {task_resources[task]['mem']}
                }}
                """
                )
                out.write(task_string)

    elif engine == "CWL":
        raise ValueError("--write-config does not currently support CWL workflows")
    elif engine == "WDL":
        raise ValueError("--write-config does not currently support WDL workflows")
    else:
        raise ValueError("Unknown workflow engine")
