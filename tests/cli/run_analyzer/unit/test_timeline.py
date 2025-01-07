from datetime import datetime

import omics.cli.run_analyzer.timeline as timeline


def test_parse_time_str():
    # Test datetime input
    dt = datetime(2023, 1, 1, 12, 0, 0)
    assert timeline._parse_time_str(dt) == dt

    # Test string with milliseconds
    time_str = "2023-01-01T12:00:00.123Z"
    expected = datetime(2023, 1, 1, 12, 0, 0, 123000)
    assert timeline._parse_time_str(time_str) == expected

    # Test string without milliseconds
    time_str = "2023-01-01T12:00:00Z"
    expected = datetime(2023, 1, 1, 12, 0, 0)
    assert timeline._parse_time_str(time_str) == expected


def test_time_factors():
    assert timeline.TIME_SCALE_FACTORS["min"] == 1 / 60, "Minutes scale factor incorrect"
    assert timeline.TIME_SCALE_FACTORS["hr"] == 1 / 3600, "Hours scale factor incorrect"
    assert timeline.TIME_SCALE_FACTORS["day"] == 1 / 86400, "Days scale factor incorrect"
    assert timeline.TIME_SCALE_FACTORS["sec"] == 1, "Seconds scale factor incorrect"


def test_get_task_timings_data():
    # Test empty task list
    assert timeline._get_task_timings_data([]).empty

    # Test tasks missing required timing fields
    tasks_missing_fields = [
        {"creationTime": "2023-01-01T00:00:00.000Z"},
        {"startTime": "2023-01-01T00:00:00.000Z"},
        {"stopTime": "2023-01-01T00:00:00.000Z"},
    ]
    assert timeline._get_task_timings_data(tasks_missing_fields).empty

    # Test valid tasks
    valid_tasks = [
        {
            "creationTime": "2023-01-01T00:00:00.000Z",
            "startTime": "2023-01-01T00:01:00.000Z",
            "stopTime": "2023-01-01T00:02:00.000Z",
            "status": "COMPLETED",
            "arn": "arn1",
            "name": "task1",
            "cpus": 2,
            "gpus": 1,
            "memory": 4,
            "instanceType": "t2.micro",
            "metrics": {"estimatedUSD": 0.50},
        },
        {
            "creationTime": "2023-01-01T00:02:00.000Z",
            "startTime": "2023-01-01T00:03:00.000Z",
            "stopTime": "2023-01-01T00:04:00.000Z",
            "status": "FAILED",
            "arn": "arn2",
            "name": "task2",
        },
    ]

    df = timeline._get_task_timings_data(valid_tasks)

    # Test dataframe shape and content
    assert len(df) == 2

    assert sorted(df.columns) == sorted(
        [
            "creationTime",
            "startTime",
            "stopTime",
            "cpus",
            "gpus",
            "memory",
            "instanceType",
            "y",
            "color",
            "running_left",
            "running_right",
            "running_duration",
            "starting_left",
            "starting_right",
            "starting_duration",
            "label",
            "text_x",
            "estimatedUSD",
            "arn",
            "name",
            "status",
            "metrics",
        ]
    )
    # Test default values
    assert df.iloc[1]["cpus"] == 0
    assert df.iloc[1]["gpus"] == 0
    assert df.iloc[1]["memory"] == 0
    assert df.iloc[1]["instanceType"] == "N/A"
    assert df.iloc[1]["estimatedUSD"] == 0.0

    # Test time calculations
    assert df.iloc[0]["running_duration"] == 1.0  # 1 minute
    assert df.iloc[0]["starting_duration"] == 1.0  # 1 minute

    # Test color mapping
    assert df.iloc[0]["color"] == timeline.TASK_COLORS["COMPLETED"]
    assert df.iloc[1]["color"] == timeline.TASK_COLORS["FAILED"]

    # Test with different time units
    df_hours = timeline._get_task_timings_data(valid_tasks, time_units="hr")
    assert df_hours.iloc[0]["running_duration"] == 1.0 / 60  # 1 minute in hours
