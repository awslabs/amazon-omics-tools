import unittest

from omics.common.omics_file_types import ExtendedEnum, OmicsFileType
from omics.transfer import FileDownload


class NumberEnum(ExtendedEnum):
    ONE = "ONE"
    TWO = "TWO"
    THREE = "THREE"


class TestExtendedEnum(unittest.TestCase):
    def test_from_object_with_valid_string(self):
        enum = NumberEnum.from_object("ONE")
        self.assertEqual(enum, NumberEnum.ONE)

    def test_from_object_with_invalid_string(self):
        with self.assertRaises(AttributeError):
            NumberEnum.from_object("GOOGLE")

    def test_from_object_with_integer(self):
        with self.assertRaises(AttributeError):
            NumberEnum.from_object(1)


class TestFileTransfer(unittest.TestCase):
    def test_init_requires_filename(self):
        with self.assertRaises(AttributeError):
            FileDownload(
                store_id="mock-store-id",
                file_set_id="mock-file-set-id",
                filename=None,
                fileobj="mock-fileobj",
                omics_file_type=OmicsFileType.READSET,
            )
