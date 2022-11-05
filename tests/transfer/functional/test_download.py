import copy
import glob
import os
import tempfile
from io import BytesIO

from botocore.exceptions import ClientError
from s3transfer.exceptions import RetriesExceededError

from omics.transfer import OmicsFileType
from omics.transfer.config import TransferConfig
from omics.transfer.download import SOCKET_ERROR
from omics.transfer.manager import TransferManager
from tests.transfer import (
    TEST_CONSTANTS,
    TEST_CONSTANTS_REFERENCE_STORE,
    RecordingSubscriber,
    StreamWithError,
    StubbedClientTest,
)
from tests.transfer.functional import (
    add_get_read_set_metadata_response,
    add_get_read_set_responses,
)


class BaseDownloadTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.config = TransferConfig(max_request_concurrency=1)
        self._manager = TransferManager(self.client, self.config)
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")

    @property
    def manager(self):
        return self._manager

    def add_n_retryable_download_file_responses(
        self, omics_file_type: OmicsFileType, n: int, num_reads: int = 0
    ):
        content = (
            TEST_CONSTANTS["content"]
            if omics_file_type.READ_SET
            else TEST_CONSTANTS_REFERENCE_STORE["content"]
        )
        method = "get_read_set" if omics_file_type.READ_SET else "get_reference"

        for _ in range(n):
            self.stubber.add_response(
                method=method,
                service_response={
                    "payload": StreamWithError(
                        copy.deepcopy(BytesIO(content)), SOCKET_ERROR, num_reads
                    )
                },
            )

    def test_download_file_does_not_exist(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        future = self.manager._download_file(
            OmicsFileType.READ_SET,
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            TEST_CONSTANTS["file"],
            self.filename,
        )
        future.result()

        self.assertTrue(os.path.exists(self.filename))
        possible_matches = glob.glob("'%s*'" % self.filename + os.extsep)
        self.assertEqual(possible_matches, [])

    def test_download_file_for_fileobj(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        with open(self.filename, "wb") as f:
            future = self.manager._download_file(
                OmicsFileType.READ_SET,
                TEST_CONSTANTS["sequence_store_id"],
                TEST_CONSTANTS["read_set_id"],
                TEST_CONSTANTS["file"],
                f,
            )
            future.result()

        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_download_file_for_seekable_filelike_obj(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        # Create a file-like object to test. In this case, it is a BytesIO object.
        bytes_io = BytesIO()
        future = self.manager._download_file(
            OmicsFileType.READ_SET,
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            TEST_CONSTANTS["file"],
            bytes_io,
        )
        future.result()

        bytes_io.seek(0)
        self.assertEqual(TEST_CONSTANTS["content"], bytes_io.read())

    def test_download_file_cleanup_on_failure(self):
        add_get_read_set_metadata_response(self.stubber)
        # Throw an error on the download
        self.stubber.add_client_error("get_read_set")

        future = self.manager._download_file(
            OmicsFileType.READ_SET,
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            TEST_CONSTANTS["file"],
            self.filename,
        )
        with self.assertRaises(ClientError):
            future.result()

        # Make sure the actual file and the temporary do not exist
        # by globbing for the file and any of its extensions
        possible_matches = glob.glob("'%s*'" % self.filename)
        self.assertEqual(possible_matches, [])

    def test_download_file_with_nonexistent_directory(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        fileobj = os.path.join(self.tempdir, "missing-directory", "test_file")
        future = self.manager._download_file(
            OmicsFileType.READ_SET,
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            TEST_CONSTANTS["file"],
            fileobj,
        )
        with self.assertRaises(IOError):
            future.result()

    def test_download_file_retries_and_succeeds(self):
        add_get_read_set_metadata_response(self.stubber)
        # Insert a response that will trigger a retry.
        self.add_n_retryable_download_file_responses(OmicsFileType.READ_SET, 1)
        # Add the normal responses to simulate the download proceeding
        # as normal after the retry.
        add_get_read_set_responses(self.stubber)

        future = self.manager._download_file(
            OmicsFileType.READ_SET,
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            TEST_CONSTANTS["file"],
            self.filename,
        )
        future.result()

        # The retry should have been consumed and the process should have
        # continued using the successful responses.
        self.stubber.assert_no_pending_responses()
        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_download_read_set_retry_failure(self):
        add_get_read_set_metadata_response(self.stubber)

        # Add responses that fill up the maximum number of retries.
        self.add_n_retryable_download_file_responses(
            OmicsFileType.READ_SET, self.config.num_download_attempts
        )

        future = self.manager._download_file(
            OmicsFileType.READ_SET,
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            TEST_CONSTANTS["file"],
            self.filename,
        )
        with self.assertRaises(RetriesExceededError):
            future.result()

        self.stubber.assert_no_pending_responses()

    def test_download_file_retry_rewinds_callbacks(self):
        add_get_read_set_metadata_response(self.stubber)
        # Insert a response that will trigger a retry after one read of the stream has been made.
        self.add_n_retryable_download_file_responses(OmicsFileType.READ_SET, 1, num_reads=1)

        # Add the normal responses to simulate the download proceeding
        # as normal after the retry.
        add_get_read_set_responses(self.stubber)

        recorder_subscriber = RecordingSubscriber()
        # Set the streaming to a size that is smaller than the data we
        # currently provide to it to simulate rewinds of callbacks.
        self.config.io_chunksize = 1

        testConfig = TransferConfig(max_request_concurrency=1, io_chunksize=1)
        self._manager = TransferManager(self.client, testConfig)

        future = self.manager._download_file(
            OmicsFileType.READ_SET,
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            TEST_CONSTANTS["file"],
            self.filename,
            [recorder_subscriber],
        )
        future.result()

        self.stubber.assert_no_pending_responses()
        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

        self.assertEqual(recorder_subscriber.calculate_bytes_seen(), len(TEST_CONSTANTS["content"]))

        # Also ensure that the second progress invocation was negative one
        # because a retry happened on the second read of the stream and we
        # know that the chunk size for each read is one.
        progress_byte_list = [
            call["bytes_transferred"] for call in recorder_subscriber.on_progress_calls
        ]
        self.assertEqual(-1, progress_byte_list[1])
