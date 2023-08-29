#!/usr/bin/env python3

import re

from omics.common.omics_file_types import (
    OMICS_URI_TYPE_DEFAULT_FILENAME_MAP,
    OMICS_URI_TYPE_FILENAME_MAP,
    OmicsFileType,
)


class OmicsUri:
    """Class for representing an Omics URI.

    Supported format:
    omics://<AWS_ACCOUNT_ID>.storage.<AWS_REGION>.amazonaws.com/<REFERENCE_STORE_ID>/reference/<REFERENCE_ID>/source

    ex: omics://429915189008.storage.us-east-1.amazonaws.com/1981413158/readSet/5346184667/source1
    ex: omics://429915189008.storage.us-east-1.amazonaws.com/1981413158/reference/5346184667/source
    """

    URI_REGEX = r"omics://(\d{10,12})\.storage\.([a-z]{2}-[a-z-]{4,}-\d+)\.amazonaws\.com/(\d{10,36})/(readSet|reference)/(\d{10,36})(/(source[12]?|index))?$"

    def __init__(self, omics_uri):
        """Initialize an Omics URI.

        Args:
            omics_uri: String representing an omics URI.
        """
        uri_match = re.match(self.URI_REGEX, omics_uri, re.IGNORECASE)
        if not uri_match:
            raise ValueError(f"Invalid URI format: {omics_uri}")
        self.account_id = uri_match.group(1)
        self.region = uri_match.group(2).lower()
        self.store_id = uri_match.group(3)
        self.resource_type = uri_match.group(4).upper()
        self.resource_id = uri_match.group(5)
        file_name = (uri_match.group(7) or "").upper()
        file_type = OmicsFileType(self.resource_type)
        if not file_name:
            self.file_name = OMICS_URI_TYPE_DEFAULT_FILENAME_MAP[file_type].value
        elif file_name in OMICS_URI_TYPE_FILENAME_MAP[file_type].list():
            self.file_name = file_name
        else:
            raise ValueError(f"Invalid URI file: {file_name}")


class OmicsUriParser:
    """URI parser for Omics-related resources."""

    def __init__(self, uri):
        """
        URI parser interface.

        :type uri: str
        :param uri: URI to parse
        """
        self._uri = uri

    def parse(self):
        """Return an object of type OmicsUri, or exception if uri can't be parsed."""
        return OmicsUri(self._uri)
