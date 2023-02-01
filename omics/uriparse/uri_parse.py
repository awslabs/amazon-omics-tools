#!/usr/bin/env python3

from urllib.parse import urlparse

from omics.common.omics_file_types import OMICS_URI_TYPE_FILENAME_MAP, OmicsFileType


class OmicsUri:
    """Class for representing an Omics URI.

    Supported format:
    omics://<AWS_ACCOUNT_ID>.storage.<AWS_REGION>.amazonaws.com/<REFERENCE_STORE_ID>/reference/<REFERENCE_ID>/source

    ex: omics://429915189008.storage.us-east-1.amazonaws.com/1981413158/readSet/5346184667/source1
    ex: omics://429915189008.storage.us-east-1.amazonaws.com/1981413158/reference/5346184667/source
    """

    SCHEME = "omics"
    NETLOC = "STORAGE"
    DEFAULT_FILE_NAME = None

    def __init__(self, omics_uri):
        """Initialize an Omics URI.

        Args:
            omics_uri: String representing an omics URI.
        """
        # https://docs.python.org/3/library/urllib.parse.html
        url_parts = urlparse(omics_uri.upper())
        if url_parts.scheme != self.SCHEME:
            raise ValueError(f"Invalid URI scheme, expected {self.SCHEME}")

        if self.NETLOC not in url_parts.netloc:
            raise ValueError(f"Invalid URI netloc, expected {self.NETLOC}")

        self._uri_path = url_parts.path

        (
            self.store_id,
            self.resource_type,
            self.resource_id,
            self.file_name,
            *_,
        ) = self._uri_path.strip("/").split("/") + [self.DEFAULT_FILE_NAME, None]

        if self.resource_type not in OmicsFileType.list():
            raise ValueError(
                f"Invalid URI path for reference, {self.resource_type} is not a valid URI type"
            )

        if (
            self.file_name
            not in OMICS_URI_TYPE_FILENAME_MAP[OmicsFileType(self.resource_type)].list()
        ):
            raise TypeError("URI file is unsupported.")


class OmicsUriParser:
    """URI parser for Omics-related resources."""

    def __init__(self, uri):
        """
        URI parser interface.

        :type uri: str
        :param uri: URI to parse
        """
        self._uri = uri.upper()

    def parse(self):
        """Return an object of type OmicsUri, or exception if uri can't be parsed."""
        contains_valid_uri_type = False
        for omicsFileType in OmicsFileType.list():
            if omicsFileType in self._uri:
                contains_valid_uri_type = True
                break

        if contains_valid_uri_type:
            return OmicsUri(self._uri)
        else:
            raise ValueError(f"Invalid URI: {self._uri}")
