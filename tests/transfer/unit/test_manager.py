import os
import tempfile

from s3transfer.utils import OSUtils

from omics.transfer import ReadSetFileName, ReferenceFileName
from omics.transfer.manager import TransferManager
from tests.transfer import (
    TEST_CONSTANTS,
    TEST_CONSTANTS_REFERENCE_STORE,
    StubbedClientTest,
)
from tests.transfer.functional import (
    add_get_read_set_metadata_response,
    add_get_read_set_responses,
    add_get_reference_metadata_response,
    add_get_reference_responses,
)


class TestTransferManager(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self.osutils = OSUtils()
        self.transfer_manager = TransferManager(self.client)

    def test_download_read_set_single_file(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        self.transfer_manager.download_read_set_file(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            ReadSetFileName.SOURCE1,
            "test_file",
        )
        self.stubber.assert_no_pending_responses()

    def test_download_read_set_invalid_file_fails_with_exception(self):
        add_get_read_set_metadata_response(self.stubber)

        with self.assertRaises(AttributeError):
            self.transfer_manager.download_read_set_file(
                TEST_CONSTANTS["sequence_store_id"],
                TEST_CONSTANTS["read_set_id"],
                "test_file_obj",
                "invalid_file",
            )

    def test_download_reference_single_file(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        self.transfer_manager.download_reference_file(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            ReferenceFileName.SOURCE,
            "test_file",
        )
        self.stubber.assert_no_pending_responses()

    def test_download_reference_invalid_file_fails_with_exception(self):
        with self.assertRaises(AttributeError):
            self.transfer_manager.download_reference_file(
                TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                "test_file_obj",
                "invalid_file",
            )

    def test_download_with_invalid_directory(self):
        add_get_read_set_metadata_response(self.stubber, files=["source1"])
        add_get_read_set_responses(self.stubber, file="SOURCE1")

        with open(os.path.join(self.tempdir, "not-a-directory.txt"), "w") as f:
            with self.assertRaises(TypeError):
                self.transfer_manager.download_read_set(
                    TEST_CONSTANTS["sequence_store_id"],
                    TEST_CONSTANTS["read_set_id"],
                    f,
                )
