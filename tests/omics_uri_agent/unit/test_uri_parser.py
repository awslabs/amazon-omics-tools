import unittest
from omics.omics_uri_agent.uri_parser import OmicsUriParser, OmicsUri, ReadSetUri, ReferenceUri
from tests.omics_uri_agent import VALID_READSET_URI, VALID_REFERENCE_URI


class MyTestCase(unittest.TestCase):
    def test_parser_invalid_uri(self):
        with self.assertRaises(ValueError):
            OmicsUriParser("invalid").parse()

    def test_parser_valid_readset_uri(self):
        res = OmicsUriParser(VALID_READSET_URI).parse()
        assert type(res) == ReadSetUri

    def test_parser_valid_reference_uri(self):
        res = OmicsUriParser(VALID_REFERENCE_URI).parse()
        assert type(res) == ReferenceUri

    def test_invalid_uri_scheme(self):
        with self.assertRaises(ValueError):
            OmicsUri(
                "http://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1").parse()

    def test_invalid_uri_netlock(self):
        with self.assertRaises(ValueError):
            OmicsUri(
                "omics://123412341234.something.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1").parse()

    # Readset tests
    def test_readset_invalid_uri(self):
        with self.assertRaises(ValueError):
            ReadSetUri("invalid").parse()

    def test_readset_invalid_type(self):
        with self.assertRaises(ValueError):
            ReadSetUri(VALID_REFERENCE_URI)

    def test_readset_invalid_file(self):
        with self.assertRaises(TypeError):
            ReadSetUri(VALID_READSET_URI + "2")

    # Reference tests
    def test_reference_invalid_uri(self):
        with self.assertRaises(ValueError):
            ReferenceUri("invalid").parse()

    def test_reference_invalid_type(self):
        with self.assertRaises(ValueError):
            ReferenceUri(VALID_READSET_URI)

    def test_reference_invalid_file(self):
        with self.assertRaises(TypeError):
            ReferenceUri(VALID_REFERENCE_URI + "2")


if __name__ == '__main__':
    unittest.main()
