# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from enum import Enum
from typing import Dict, List

from s3transfer.utils import CallArgs

REQUIRED_FILE_METADATA_ARGS = ["content_length", "part_size", "total_parts"]


def check_required_file_metadata_present(call_args: CallArgs) -> bool:
    """Check whether the call arguments include file metadata."""
    return call_args.file_metadata and set(call_args.file_metadata).issuperset(
        REQUIRED_FILE_METADATA_ARGS
    )


def validate_all_known_args(actual: Dict, allowed: List) -> None:
    """Validate that all provided arguments are allowed."""
    for kwarg in actual:
        if kwarg not in allowed:
            raise ValueError(
                f"Invalid extra_args key {kwarg}, must be one of: {', '.join(allowed)}"
            )


def validate_and_create_directory_if_needed(directory: str) -> None:
    """Create a directory if one does not exist already."""
    if os.path.isfile(directory):
        raise ValueError("Invalid directory")
    if not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)


class ReadSetFile(Enum):
    """Enum of read set file types."""

    SOURCE1 = "SOURCE1"
    SOURCE2 = "SOURCE2"
    INDEX = "INDEX"


class ReferenceFile(Enum):
    """Enum of reference file types."""

    SOURCE = "SOURCE"
    INDEX = "INDEX"
