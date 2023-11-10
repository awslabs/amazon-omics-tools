import io
import os
import tempfile
from concurrent.futures import CancelledError
from unittest import skip

from s3transfer.exceptions import FatalError

from omics.common.omics_file_types import (
    OmicsFileType,
    ReadSetFileName,
    ReferenceFileName,
)
from omics.transfer.config import TransferConfig
from omics.transfer.manager import TransferManager
from omics.transfer.read_set_upload import MIB_BYTES
from tests.transfer import (
    TEST_CONSTANTS,
    TEST_CONSTANTS_REFERENCE_STORE,
    StubbedClientTest,
)
from tests.transfer.functional import (
    add_complete_upload_response,
    add_create_upload_response,
    add_get_read_set_metadata_response,
    add_get_read_set_responses,
    add_get_reference_metadata_response,
    add_get_reference_responses,
    add_gzipped_get_read_set_metadata_response,
    add_gzipped_get_read_set_response,
    add_upload_part_response,
)


class ArbitraryException(Exception):
    pass


class SingleThreadedTransferManagerTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self._manager = TransferManager(
            self.client,
            TransferConfig(
                # Downloading multiple files in tests only works with `TransferConfig.use_threads=False`
                # since boto.Stubber requires calls to be made in the order they are registered
                # and running this in multi-threaded mode is non-deterministic.
                use_threads=False,
                max_request_concurrency=1,
                max_submission_concurrency=1,
            ),
        )

    @property
    def manager(self):
        return self._manager

    def add_default_stubber_responses(self, file_type: OmicsFileType):
        if file_type == OmicsFileType.READSET:
            add_get_read_set_metadata_response(self.stubber, files=["source1", "source2"])
            add_get_read_set_metadata_response(self.stubber, files=["source1"])
            add_get_read_set_responses(self.stubber, file="SOURCE1")
            add_get_read_set_metadata_response(self.stubber, files=["source2"])
            add_get_read_set_responses(self.stubber, file="SOURCE2")
        elif file_type == OmicsFileType.REFERENCE:
            add_get_reference_metadata_response(self.stubber, files=["source", "index"])
            add_get_reference_metadata_response(self.stubber, files=["source"])
            add_get_reference_responses(self.stubber, file="SOURCE")
            add_get_reference_metadata_response(self.stubber, files=["index"])
            add_get_reference_responses(self.stubber, file="INDEX")

    def test_download_read_set(self):
        self.add_default_stubber_responses(OmicsFileType.READSET)

        self.manager.download_read_set(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            self.tempdir,
        )

        possible_matches = os.listdir(self.tempdir)

        self.assertEqual(len(possible_matches), 2)
        self.assertEqual(
            set(possible_matches),
            {"test-read-set_1.fastq", "test-read-set_2.fastq"},
        )

        with open(os.path.join(self.tempdir, "test-read-set_1.fastq"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

        with open(os.path.join(self.tempdir, "test-read-set_2.fastq"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_gzipped_read_set_filename_extension(self):
        add_gzipped_get_read_set_metadata_response(self.stubber, files=["source1"])
        add_gzipped_get_read_set_metadata_response(self.stubber, files=["source1"])
        add_gzipped_get_read_set_response(self.stubber, file="SOURCE1")

        self.manager.download_read_set(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            self.tempdir,
        )

        possible_matches = os.listdir(self.tempdir)

        self.assertEqual(len(possible_matches), 1)
        self.assertEqual(
            set(possible_matches),
            {"test-read-set.fastq.gz"},
        )

    def test_download_read_set_without_wait(self):
        self.add_default_stubber_responses(OmicsFileType.READSET)

        futures = self.manager.download_read_set(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            self.tempdir,
            wait=False,
        )
        for future in futures:
            future.result()
        possible_matches = os.listdir(self.tempdir)
        self.assertEqual(len(possible_matches), 2)

    def test_download_read_set_to_config_dir(self):
        new_directory = f"{self.tempdir}/test-default"
        self._manager = TransferManager(
            self.client,
            TransferConfig(
                use_threads=False,
                directory=new_directory,
                max_request_concurrency=1,
                max_submission_concurrency=1,
            ),
        )
        self.add_default_stubber_responses(OmicsFileType.READSET)

        self.manager.download_read_set(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
        )
        possible_matches = os.listdir(new_directory)
        self.assertEqual(len(possible_matches), 2)

    def test_download_reference(self):
        self.add_default_stubber_responses(OmicsFileType.REFERENCE)

        self.manager.download_reference(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            self.tempdir,
        )

        possible_matches = os.listdir(self.tempdir)
        self.assertEqual(len(possible_matches), 2)
        self.assertEqual(
            set(possible_matches),
            {"test-reference-file.fasta", "test-reference-file.fasta.fai"},
        )

        with open(os.path.join(self.tempdir, "test-reference-file.fasta"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())

        with open(os.path.join(self.tempdir, "test-reference-file.fasta.fai"), "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())

    def test_download_reference_without_wait(self):
        self.add_default_stubber_responses(OmicsFileType.REFERENCE)

        futures = self.manager.download_reference(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            self.tempdir,
            wait=False,
        )
        for future in futures:
            future.result()
        possible_matches = os.listdir(self.tempdir)
        self.assertEqual(len(possible_matches), 2)

    def test_download_reference_to_config_dir(self):
        new_directory = f"{self.tempdir}/test-default"
        self._manager = TransferManager(
            self.client,
            TransferConfig(
                use_threads=False,
                directory=new_directory,
                max_request_concurrency=1,
                max_submission_concurrency=1,
            ),
        )
        self.add_default_stubber_responses(OmicsFileType.REFERENCE)

        self.manager.download_reference(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
        )
        possible_matches = os.listdir(new_directory)
        self.assertEqual(len(possible_matches), 2)

    def test_upload_read_set(self):
        add_create_upload_response(self.stubber)
        add_upload_part_response(self.stubber, 1, ReadSetFileName.SOURCE1)
        add_upload_part_response(self.stubber, 2, ReadSetFileName.SOURCE1)
        add_upload_part_response(self.stubber, 3, ReadSetFileName.SOURCE1)
        add_complete_upload_response(self.stubber)

        read_set_id = self.manager.upload_read_set(
            io.BytesIO(os.urandom(MIB_BYTES * 250)),
            TEST_CONSTANTS["sequence_store_id"],
            "CRAM",
            "name",
            "subjectId",
            "sampleId",
            "referenceArn",
        )

        self.assertEqual(read_set_id, TEST_CONSTANTS["read_set_id"])


class MultiThreadedTransferManagerTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self._manager = TransferManager(
            self.client,
            TransferConfig(max_request_concurrency=1, max_submission_concurrency=1),
        )

    @property
    def manager(self):
        return self._manager

    def test_download_read_set_file(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        self.manager.download_read_set_file(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            ReadSetFileName.SOURCE1,
            self.filename,
        )

        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_download_reference_file(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        self.manager.download_reference_file(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            ReferenceFileName.SOURCE,
            self.filename,
        )

        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())

    def test_download_read_set_file_to_config_dir(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        new_directory = f"{self.tempdir}/test-default"
        manager = TransferManager(
            self.client,
            TransferConfig(
                use_threads=False,
                directory=new_directory,
                max_request_concurrency=1,
                max_submission_concurrency=1,
            ),
        )

        manager.download_read_set_file(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            ReadSetFileName.SOURCE1,
        )

        expected_filename = os.path.join(
            new_directory,
            "test-read-set.fastq",
        )

        with open(expected_filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    @skip("Concurrency in futures leads to out of order stubs and therefore to assertion failures")
    def test_error_in_context_manager_cancels_incomplete_transfers(self):
        num_transfers = 100
        futures = []
        ref_exception_msg = "arbitrary exception"
        for _ in range(num_transfers):
            add_get_read_set_metadata_response(self.stubber)
            add_get_read_set_responses(self.stubber)

        try:
            with self.manager:
                for i in range(num_transfers):
                    futures.append(
                        self.manager._download_file(
                            OmicsFileType.READSET,
                            TEST_CONSTANTS["sequence_store_id"],
                            TEST_CONSTANTS["read_set_id"],
                            TEST_CONSTANTS["file"],
                            self.filename,
                        )
                    )
                raise ArbitraryException(ref_exception_msg)
        except ArbitraryException:
            # At least one of the submitted futures should have been cancelled.
            with self.assertRaisesRegex(FatalError, ref_exception_msg):
                for future in futures:
                    future.result()

    @skip("Concurrency in futures leads to out of order stubs and therefore to assertion failures")
    def test_control_c_in_context_manager_cancels_incomplete_transfers(self):
        num_transfers = 100
        futures = []

        for _ in range(num_transfers):
            add_get_read_set_metadata_response(self.stubber)
            add_get_read_set_responses(self.stubber)

        try:
            with self.manager:
                for i in range(num_transfers):
                    futures.append(
                        self.manager._download_file(
                            OmicsFileType.READSET,
                            TEST_CONSTANTS["sequence_store_id"],
                            TEST_CONSTANTS["read_set_id"],
                            TEST_CONSTANTS["file"],
                            self.filename,
                        )
                    )
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            with self.assertRaisesRegex(CancelledError, "KeyboardInterrupt()"):
                for future in futures:
                    future.result()

    def test_upload_read_set(self):
        add_create_upload_response(self.stubber)
        add_upload_part_response(self.stubber, 1, ReadSetFileName.SOURCE1)
        add_complete_upload_response(self.stubber)

        read_set_id = self.manager.upload_read_set(
            io.BytesIO(os.urandom(MIB_BYTES)),
            TEST_CONSTANTS["sequence_store_id"],
            "CRAM",
            "name",
            "subjectId",
            "sampleId",
            "referenceArn",
        )

        self.assertEqual(read_set_id, TEST_CONSTANTS["read_set_id"])
