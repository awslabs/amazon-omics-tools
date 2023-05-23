import unittest

from omics.uriparse.uri_parse import OmicsUri, OmicsUriParser
from tests.uriparse import VALID_READSET_URI, VALID_REFERENCE_URI
from tests.uriparse import VALID_READSET_DEFAULT_FILE_URI, VALID_REFERENCE_DEFAULT_FILE_URI


class UriParserTest(unittest.TestCase):
    def test_parser_invalid_uri(self):
        with self.assertRaises(ValueError):
            OmicsUriParser("invalid").parse()

    def test_parser_valid_readset_uri(self):
        res = OmicsUriParser(VALID_READSET_URI).parse()
        assert res.resource_type == "READSET"
        assert res.file_name == "SOURCE2"
        assert res.region == "us-east-1"
        assert res.account_id == "123412341234"

    def test_parser_valid_readset_default_file_uri(self):
        res = OmicsUriParser(VALID_READSET_DEFAULT_FILE_URI).parse()
        assert res.resource_type == "READSET"
        assert res.file_name == "SOURCE1"
        assert res.region == "us-west-2"
        assert res.account_id == "123412341234"

    def test_parser_valid_reference_uri(self):
        res = OmicsUriParser(VALID_REFERENCE_URI).parse()
        assert res.resource_type == "REFERENCE"
        assert res.file_name == "INDEX"
        assert res.region == "us-east-1"
        assert res.account_id == "123412341234"

    def test_parser_valid_reference_default_file_uri(self):
        res = OmicsUriParser(VALID_REFERENCE_DEFAULT_FILE_URI).parse()
        assert res.resource_type == "REFERENCE"
        assert res.file_name == "SOURCE"
        assert res.region == "us-west-2"
        assert res.account_id == "123412341234"

    def test_invalid_uri_scheme(self):
        with self.assertRaises(ValueError):
            OmicsUri(
                "http://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1"
            )

    def test_invalid_uri_netlock(self):
        with self.assertRaises(ValueError):
            OmicsUri(
                "omics://123412341234.something.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1"
            )

    def test_invalid_uri(self):
        invalid_uri = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/invalid/5346184667/source"
        with self.assertRaises(ValueError):
            OmicsUri(invalid_uri)

    # Readset tests
    def test_readset_invalid_id(self):
        invalid_uri = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/"
        with self.assertRaises(ValueError):
            OmicsUri(invalid_uri)

    def test_readset_invalid_file(self):
        INVALID_READSET_URI = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source"
        with self.assertRaises(ValueError):
            OmicsUri(VALID_READSET_URI + "2")
        with self.assertRaises(ValueError):
            OmicsUri(INVALID_READSET_URI)

    # Reference tests
    def test_reference_invalid_id(self):
        invalid_uri = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/reference/"
        with self.assertRaises(ValueError):
            OmicsUri(invalid_uri)

    def test_reference_invalid_file(self):
        INVALID_REFERENCE_URI = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/reference/5346184667/source2"
        with self.assertRaises(ValueError):
            OmicsUri(VALID_REFERENCE_URI + "2")
        with self.assertRaises(ValueError):
            OmicsUri(INVALID_REFERENCE_URI)


if __name__ == "__main__":
    unittest.main()
