#!/usr/bin/env python3

from urllib.parse import urlparse
from omics.transfer import (
    ReadSetFileName,
    ReferenceFileName
)


# Example URIs supported:
# omics://429915189008.storage.us-east-1.amazonaws.com/1981413158/readSet/5346184667/source1
# omics://429915189008.storage.us-east-1.amazonaws.com/1981413158/reference/5346184667/source
class OmicsUri:
    SCHEME = "omics"
    NETLOC = "STORAGE"
    IDENTIFIER = ""
    DEFAULT_FILE_NAME = None

    def __init__(self, omics_uri):
        # https://docs.python.org/3/library/urllib.parse.html
        url_parts = urlparse(omics_uri.upper())
        if url_parts.scheme != self.SCHEME:
            raise ValueError(f"Invalid URI scheme, expected {self.SCHEME}")

        if self.NETLOC not in url_parts.netloc:
            raise ValueError(f"Invalid URI netloc, expected {self.NETLOC}")

        self._uri_path = url_parts.path

        self.store_id, self.resource_type, self.resource_id, self.file_name, *_ =\
            self._uri_path.strip("/").split("/") + [self.DEFAULT_FILE_NAME, None]

        if self.resource_type != self.IDENTIFIER:
            raise ValueError(f"Invalid URI path for reference, {self.resource_type} is not {self.IDENTIFIER}")


class ReadSetUri(OmicsUri):
    IDENTIFIER = "READSET"
    DEFAULT_FILE_NAME = ReadSetFileName.SOURCE1.name

    def __init__(self, omics_uri):
        print("Starting readset instance...")
        super().__init__(omics_uri)

        try:
            self.readset_file = ReadSetFileName(self.file_name)
        except ValueError:
            raise TypeError("URI file is unsupported.")


class ReferenceUri(OmicsUri):
    IDENTIFIER = "REFERENCE"
    DEFAULT_FILE_NAME = ReferenceFileName.SOURCE.name

    def __init__(self, omics_uri):
        print("Starting reference instance...")
        super().__init__(omics_uri)

        try:
            self.reference_file = ReferenceFileName(self.file_name)
        except ValueError:
            raise TypeError("URI file is unsupported.")


class OmicsUriParser:
    """
    URI parser for Omics-related resources
    """

    def __init__(self, uri):
        """
        URI parser interface

        :type uri: str
        :param uri: URI to parse
        """
        self._uri = uri.upper()

    def parse(self):
        """
        Returns an object of type readSet or reference depending on thr uri initial parsing result, or exception
         if uri can't be parsed

        :return:
        """
        if ReadSetUri.IDENTIFIER in self._uri:
            return ReadSetUri(self._uri)
        elif ReferenceUri.IDENTIFIER in self._uri:
            return ReferenceUri(self._uri)
        else:
            raise ValueError(f"Invalid URI: {self._uri}")