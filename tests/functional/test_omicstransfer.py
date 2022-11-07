import os
import tempfile

from omics_transfer import OmicsTransfer
from omics_transfer.manager import OmicsTransferConfig
from tests import TEST_CONSTANTS, TEST_CONSTANTS_REFERENCE_STORE, StubbedClientTest
from tests.functional import (
    add_get_readset_metadata_response,
    add_get_readset_responses,
    add_get_reference_metadata_response,
    add_get_reference_responses,
    create_download_readset_call_kwargs,
    create_download_reference_call_kwargs,
)


class OmicsTransferTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.config = OmicsTransferConfig(max_request_concurrency=1, max_submission_concurrency=1)
        self._omics_transfer = OmicsTransfer(self.client, self.config)
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")

    @property
    def omics_transfer(self):
        return self._omics_transfer

    def test_download_readset(self):
        add_get_readset_metadata_response(self.stubber)
        add_get_readset_responses(self.stubber)

        self.omics_transfer.download_readset(**create_download_readset_call_kwargs(self.filename))

        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_download_readset_all(self):
        add_get_readset_metadata_response(self.stubber, files=["source1", "source2"])
        add_get_readset_responses(self.stubber, file="SOURCE1")
        add_get_readset_responses(self.stubber, file="SOURCE2")

        self.omics_transfer.download_readset_all(
            TEST_CONSTANTS["sequence_store_id"], TEST_CONSTANTS["read_set_id"], self.tempdir
        )

        possible_matches = os.listdir(self.tempdir)
        self.assertEqual(len(possible_matches), 2)
        self.assertEqual(
            set(possible_matches),
            {"source1_1234567890_0987654321", "source2_1234567890_0987654321"},
        )

        with open(os.path.join(self.tempdir, "source1_1234567890_0987654321"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())
        with open(os.path.join(self.tempdir, "source2_1234567890_0987654321"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_download_reference(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        self.omics_transfer.download_reference(
            **create_download_reference_call_kwargs(self.filename)
        )

        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())

    def test_download_reference_all(self):
        add_get_reference_metadata_response(self.stubber, files=["source", "index"])
        add_get_reference_responses(self.stubber, file="SOURCE")
        add_get_reference_responses(self.stubber, file="INDEX")

        self.omics_transfer.download_reference_all(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            self.tempdir,
        )

        possible_matches = os.listdir(self.tempdir)
        self.assertEqual(len(possible_matches), 2)
        self.assertEqual(
            set(possible_matches), {"source_1234567890_0987654321", "index_1234567890_0987654321"}
        )

        with open(os.path.join(self.tempdir, "source_1234567890_0987654321"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())
        with open(os.path.join(self.tempdir, "index_1234567890_0987654321"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())
