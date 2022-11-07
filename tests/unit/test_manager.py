import unittest

from omics_transfer.manager import OmicsTransferConfig, OmicsTransferManager
from tests import TEST_CONSTANTS, TEST_CONSTANTS_REFERENCE_STORE, StubbedClientTest


class TestOmicsTransferConfig(unittest.TestCase):
    def test_exception_on_zero_attr_value(self):
        with self.assertRaises(ValueError):
            OmicsTransferConfig(max_request_queue_size=0)

    def test_exception_on_negative_attr_value(self):
        with self.assertRaises(ValueError):
            OmicsTransferConfig(max_request_concurrency=-10)


class TestOmicsTransferManager(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.transfer_manager = OmicsTransferManager(self.client)

    def add_get_readset_metadata_response(self, files=None):
        if files is None:
            files = ["source1"]
        file_metadata = {}
        for file in files:
            file_metadata[file] = {
                "contentLength": len(TEST_CONSTANTS["content"]),
                "partSize": TEST_CONSTANTS["part_size"],
                "totalParts": TEST_CONSTANTS["total_parts"],
            }
        self.stubber.add_response(
            "get_read_set_metadata",
            {
                "arn": "arn:aws:omics:us-west-2:123456789012:sequenceStore/1234567890/readSet/1234567890",
                "creationTime": "2022-06-21T16:30:32Z",
                "id": TEST_CONSTANTS["read_set_id"],
                "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
                "status": "ACTIVE",
                "fileType": "FASTQ",
                "files": file_metadata,
            },
        )

    def add_get_reference_metadata_response(self, files=None):
        if files is None:
            files = ["source"]
        file_metadata = {}
        for file in files:
            file_metadata[file] = {
                "contentLength": len(TEST_CONSTANTS["content"]),
                "partSize": TEST_CONSTANTS["part_size"],
                "totalParts": TEST_CONSTANTS["total_parts"],
            }
        self.stubber.add_response(
            "get_reference_metadata",
            {
                "arn": "arn:aws:omics:us-west-2:123456789012:referenceStore/1234567890/reference/1234567890",
                "creationTime": "2022-06-21T16:30:32Z",
                "updateTime": "2022-06-22T18:30:32Z",
                "id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                "referenceStoreId": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                "status": "ACTIVE",
                "files": file_metadata,
                "md5": "eb247690435415724f20d8702e011966",
            },
        )

    def test_download_readset_single_file(self):
        self.add_get_readset_metadata_response()
        self.transfer_manager.download_readset(
            TEST_CONSTANTS["sequence_store_id"], TEST_CONSTANTS["read_set_id"], "test_file"
        )
        self.stubber.assert_no_pending_responses()

    def test_download_readset_invalid_file_fails_with_exception(self):
        with self.assertRaises(ValueError):
            self.transfer_manager.download_readset(
                TEST_CONSTANTS["sequence_store_id"],
                TEST_CONSTANTS["read_set_id"],
                "test_file_obj",
                "invalid_file",
            )

    def test_download_reference_single_file(self):
        self.add_get_reference_metadata_response()
        self.transfer_manager.download_reference(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            "test_file",
        )
        self.stubber.assert_no_pending_responses()

    def test_download_reference_invalid_file_fails_with_exception(self):
        with self.assertRaises(ValueError):
            self.transfer_manager.download_reference(
                TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                "test_file_obj",
                "invalid_file",
            )
