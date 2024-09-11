import sys
from datetime import datetime

import pandas as pd  # type: ignore
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Div, Range1d
from bokeh.plotting import figure, show

TIME_SCALE_FACTORS = {"sec": 1, "min": 1 / 60, "hr": 1 / 3600, "day": 1 / 86400}

TASK_COLORS = {"COMPLETED": "cornflowerblue", "FAILED": "crimson", "CANCELLED": "orange"}


def _parse_time_str(time_str):
    # if time_str is actually a datetime just return it
    if isinstance(time_str, datetime):
        return time_str

    try:
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S%fZ")


def _get_task_timings_data(tasks, time_units="min"):
    time_scale_factor = TIME_SCALE_FACTORS[time_units]

    # remove tasks that don't have a creationTime
    filtered_tasks = [task for task in tasks if task.get("creationTime")]
    if not filtered_tasks:
        print("No tasks found with timing data, a plot cannot be created", file=sys.stderr)
        return pd.DataFrame()

    tare = min([_parse_time_str(task["creationTime"]) for task in filtered_tasks])

    for i, task in enumerate(tasks):
        if "creationTime" not in task:
            task["creationTime"] = tare
            task["startTime"] = task["creationTime"]
            task["stopTime"] = task["creationTime"]
        else:
            task["creationTime"] = _parse_time_str(task["creationTime"])
            task["startTime"] = _parse_time_str(task["startTime"])
            task["stopTime"] = _parse_time_str(task["stopTime"])
        task["cpus"] = task.get("cpus", 0)
        task["gpus"] = task.get("gpus", 0)
        task["memory"] = task.get("memory", 0)
        task["instanceType"] = task.get("instanceType", "N/A")

        task["y"] = i
        task["color"] = TASK_COLORS[task["status"]]

        task["running_left"] = (task["startTime"] - tare).total_seconds() * time_scale_factor
        task["running_right"] = (task["stopTime"] - tare).total_seconds() * time_scale_factor
        task["running_duration"] = task["running_right"] - task["running_left"]

        task["starting_left"] = (task["creationTime"] - tare).total_seconds() * time_scale_factor
        task["starting_right"] = task["running_left"]
        task["starting_duration"] = task["starting_right"] - task["starting_left"]

        task["label"] = f"({task['arn']}) {task['name']}"
        task["text_x"] = (task["stopTime"] - tare).total_seconds() + 30 * time_scale_factor

        tasks[i] = task
        task["estimatedUSD"] = task.get("metrics", {}).get("estimatedUSD", 0.0)

    return pd.DataFrame.from_records(tasks).sort_values("creationTime")


def plot_timeline(tasks, title="", time_units="min", max_duration_hrs=5, show_plot=True):
    """Plot a time line figure for supplied tasks"""
    time_scale_factor = TIME_SCALE_FACTORS[time_units]
    data = _get_task_timings_data(tasks, time_units=time_units)

    source = ColumnDataSource(data)

    tooltips = [
        ("taskId", "@arn"),
        ("name", "@name"),
        ("cpus", "@cpus"),
        ("gpus", "@gpus"),
        ("memory", "@memory GiB"),
        ("instanceType", "@instanceType"),
        ("starting", f"@starting_duration {time_units}"),
        ("duration", f"@running_duration {time_units}"),
        ("status", "@status"),
        ("est. cost USD", "@estimatedUSD{0.00000}"),
    ]

    p_run = figure(width=960, height=800, sizing_mode="stretch_both", tooltips=tooltips)
    p_run.hbar(
        # start time bar
        y="y",
        left="starting_left",
        right="starting_right",
        height=0.8,
        color="lightgrey",
        source=source,
        legend_label="starting",
    )
    p_run.hbar(
        # running time bar
        y="y",
        left="running_left",
        right="running_right",
        height=0.8,
        color="color",
        source=source,
        legend_label="running",
    )
    if len(data) < 101:
        p_run.text(
            # task name label
            color="black",
            x="running_right",
            x_offset=10,
            y="y",
            text="name",
            alpha=0.4,
            text_baseline="middle",
            text_font_size="1.5ex",
            source=source,
        )
    x_max = max_duration_hrs * 3600 * time_scale_factor  # max expected workflow duration in hours
    x_min = -(x_max * 0.05)
    p_run.x_range = Range1d(x_min, x_max)
    p_run.y_range.flipped = False
    p_run.xaxis.axis_label = f"task execution time ({time_units})"
    p_run.yaxis.visible = False
    p_run.legend.location = "top_right"
    max_stop_time = data["stopTime"].max()
    min_creation_time = data["creationTime"].min()
    p_run.title.text = (
        f"{title}, "
        f"tasks: {len(tasks)}, "
        f"wall time: {(_parse_time_str(max_stop_time) - _parse_time_str(min_creation_time)).total_seconds() * time_scale_factor:.2f} {time_units}"
    )

    layout = column(Div(text=f"<strong>{title}</strong>"), p_run)

    if show_plot:
        show(p_run)

    return layout
