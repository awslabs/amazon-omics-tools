import unittest

from omics.uriparse.uri_parse import OmicsUri, OmicsUriParser
from tests.uriparse import VALID_READSET_URI, VALID_REFERENCE_URI


class UriParserTest(unittest.TestCase):
    def test_parser_invalid_uri(self):
        with self.assertRaises(ValueError):
            OmicsUriParser("invalid").parse()

    def test_parser_valid_readset_uri(self):
        res = OmicsUriParser(VALID_READSET_URI).parse()
        assert res.resource_type == "READSET"
        assert res.file_name == "SOURCE2"

    def test_parser_valid_reference_uri(self):
        res = OmicsUriParser(VALID_REFERENCE_URI).parse()
        assert res.resource_type == "REFERENCE"
        assert res.file_name == "INDEX"

    def test_invalid_uri_scheme(self):
        with self.assertRaises(ValueError):
            OmicsUri(
                "http://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1"
            ).parse()

    def test_invalid_uri_netlock(self):
        with self.assertRaises(ValueError):
            OmicsUri(
                "omics://123412341234.something.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1"
            ).parse()

    # Readset tests
    def test_readset_invalid_uri(self):
        with self.assertRaises(ValueError):
            OmicsUri("invalid").parse()

    def test_readset_invalid_file(self):
        INVALID_READSET_URI = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source"
        with self.assertRaises(TypeError):
            OmicsUri(VALID_READSET_URI + "2")
        with self.assertRaises(TypeError):
            OmicsUri(INVALID_READSET_URI)

    # Reference tests
    def test_reference_invalid_uri(self):
        with self.assertRaises(ValueError):
            OmicsUri("invalid").parse()

    def test_reference_invalid_file(self):
        INVALID_REFERENCE_URI = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/reference/5346184667/source2"
        with self.assertRaises(TypeError):
            OmicsUri(VALID_REFERENCE_URI + "2")
        with self.assertRaises(TypeError):
            OmicsUri(INVALID_REFERENCE_URI)


if __name__ == "__main__":
    unittest.main()
