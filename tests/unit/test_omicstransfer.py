import os
import tempfile
from unittest import mock

from s3transfer.utils import OSUtils

from omics_transfer import OmicsTransfer
from omics_transfer.utils import ReadSetFile, ReferenceFile
from tests import TEST_CONSTANTS, TEST_CONSTANTS_REFERENCE_STORE, StubbedClientTest


class TestOmicsTransfer(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self.client = mock.Mock()
        self.osutils = OSUtils()

    def test_download_readset_single_file(self):
        with mock.patch(
            "omics_transfer.omicstransfer.OmicsTransferManager.download_readset"
        ) as download_readset:
            omics_transfer = OmicsTransfer(self.client)
            omics_transfer.download_readset(
                TEST_CONSTANTS["sequence_store_id"], TEST_CONSTANTS["read_set_id"], self.filename
            )

            download_readset.assert_called_with(
                TEST_CONSTANTS["sequence_store_id"],
                TEST_CONSTANTS["read_set_id"],
                self.filename,
                file=ReadSetFile.SOURCE1,
                subscribers=None,
            )
            assert download_readset.call_count == 1

    def test_download_all_available_readset_files(self):
        with mock.patch(
            "omics_transfer.omicstransfer.OmicsTransferManager.download_readset"
        ) as download_readset:
            omics_transfer = OmicsTransfer(self.client)
            self.client.get_read_set_metadata.return_value = (
                self._add_get_readset_metadata_response()
            )
            omics_transfer.download_readset_all(
                TEST_CONSTANTS["sequence_store_id"], TEST_CONSTANTS["read_set_id"], self.tempdir
            )

            expected_file_metadata = {
                "part_size": TEST_CONSTANTS["part_size"],
                "total_parts": TEST_CONSTANTS["total_parts"],
                "content_length": len(TEST_CONSTANTS["content"]),
            }
            download_readset.assert_has_calls(
                [
                    self._assert_transfer_manager_called_with_for_readset(
                        os.path.join(
                            self.tempdir,
                            "_".join(
                                [
                                    ReadSetFile.SOURCE1.value.lower(),
                                    TEST_CONSTANTS["sequence_store_id"],
                                    TEST_CONSTANTS["read_set_id"],
                                ]
                            ),
                        ),
                        file=ReadSetFile.SOURCE1,
                        file_metadata=expected_file_metadata,
                    ),
                    self._assert_transfer_manager_called_with_for_readset(
                        os.path.join(
                            self.tempdir,
                            "_".join(
                                [
                                    ReadSetFile.INDEX.value.lower(),
                                    TEST_CONSTANTS["sequence_store_id"],
                                    TEST_CONSTANTS["read_set_id"],
                                ]
                            ),
                        ),
                        file=ReadSetFile.INDEX,
                        file_metadata=expected_file_metadata,
                    ),
                ]
            )
            assert download_readset.call_count == 2

    def test_download_all_readset_files_with_invalid_directory_arg(self):
        with self.assertRaises(ValueError):
            omics_transfer = OmicsTransfer(self.client)
            self.client.get_read_set_metadata.return_value = (
                self._add_get_readset_metadata_response()
            )
            self.osutils.allocate(self.filename, 1)
            omics_transfer.download_readset_all(
                TEST_CONSTANTS["sequence_store_id"], TEST_CONSTANTS["read_set_id"], self.filename
            )

    def test_download_reference_single_file(self):
        with mock.patch(
            "omics_transfer.omicstransfer.OmicsTransferManager.download_reference"
        ) as download_reference:
            omics_transfer = OmicsTransfer(self.client)
            omics_transfer.download_reference(
                TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                self.filename,
            )

            download_reference.assert_called_with(
                TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                self.filename,
                file=ReferenceFile.SOURCE,
                subscribers=None,
            )
            assert download_reference.call_count == 1

    def test_download_all_available_reference_files(self):
        with mock.patch(
            "omics_transfer.omicstransfer.OmicsTransferManager.download_reference"
        ) as download_reference:
            omics_transfer = OmicsTransfer(self.client)
            self.client.get_reference_metadata.return_value = (
                self._add_get_reference_metadata_response()
            )
            omics_transfer.download_reference_all(
                TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                self.tempdir,
            )

            expected_file_metadata = {
                "part_size": TEST_CONSTANTS_REFERENCE_STORE["part_size"],
                "total_parts": TEST_CONSTANTS_REFERENCE_STORE["total_parts"],
                "content_length": len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
            }
            download_reference.assert_has_calls(
                [
                    self._assert_transfer_manager_called_with_for_reference(
                        os.path.join(
                            self.tempdir,
                            "_".join(
                                [
                                    ReferenceFile.SOURCE.value.lower(),
                                    TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                                    TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                                ]
                            ),
                        ),
                        file=ReferenceFile.SOURCE,
                        file_metadata=expected_file_metadata,
                    ),
                    self._assert_transfer_manager_called_with_for_reference(
                        os.path.join(
                            self.tempdir,
                            "_".join(
                                [
                                    ReferenceFile.INDEX.value.lower(),
                                    TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                                    TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                                ]
                            ),
                        ),
                        file=ReferenceFile.INDEX,
                        file_metadata=expected_file_metadata,
                    ),
                ]
            )
            assert download_reference.call_count == 2

    def test_download_all_reference_files_with_invalid_directory_arg(self):
        with self.assertRaises(ValueError):
            omics_transfer = OmicsTransfer(self.client)
            self.client.get_reference_metadata.return_value = (
                self._add_get_reference_metadata_response()
            )
            self.osutils.allocate(self.filename, 1)
            omics_transfer.download_reference_all(
                TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                self.filename,
            )

    def _add_get_readset_metadata_response(self):
        return {
            "arn": "test_arn",
            "creationTime": "2022-06-21T16:30:32Z",
            "id": TEST_CONSTANTS["read_set_id"],
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "status": "ACTIVE",
            "fileType": "FASTQ",
            "files": {
                "source1": {
                    "contentLength": len(TEST_CONSTANTS["content"]),
                    "partSize": TEST_CONSTANTS["part_size"],
                    "totalParts": TEST_CONSTANTS["total_parts"],
                },
                "index": {
                    "contentLength": len(TEST_CONSTANTS["content"]),
                    "partSize": TEST_CONSTANTS["part_size"],
                    "totalParts": TEST_CONSTANTS["total_parts"],
                },
            },
        }

    def _add_get_reference_metadata_response(self):
        return {
            "arn": "test_arn",
            "creationTime": "2022-06-21T16:30:32Z",
            "updateTime": "2022-06-22T18:30:32Z",
            "id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            "referenceStoreId": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            "status": "ACTIVE",
            "files": {
                "source": {
                    "contentLength": len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
                    "partSize": TEST_CONSTANTS_REFERENCE_STORE["part_size"],
                    "totalParts": TEST_CONSTANTS_REFERENCE_STORE["total_parts"],
                },
                "index": {
                    "contentLength": len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
                    "partSize": TEST_CONSTANTS_REFERENCE_STORE["part_size"],
                    "totalParts": TEST_CONSTANTS_REFERENCE_STORE["total_parts"],
                },
            },
            "md5": "eb247690435415724f20d8702e011966",
        }

    def _assert_transfer_manager_called_with_for_readset(
        self,
        fileobj,
        file=ReadSetFile.SOURCE1,
        file_metadata=None,
    ):
        return mock.call(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            fileobj,
            file=file,
            file_metadata=file_metadata,
        )

    def _assert_transfer_manager_called_with_for_reference(
        self,
        fileobj,
        file=ReferenceFile.SOURCE,
        file_metadata=None,
    ):
        return mock.call(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            fileobj,
            file=file,
            file_metadata=file_metadata,
        )
