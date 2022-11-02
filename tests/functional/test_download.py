import copy
import glob
import os
import tempfile
from io import BytesIO

from botocore.exceptions import ClientError

from omics_transfer import RetriesExceededError
from omics_transfer.download import SOCKET_ERROR
from omics_transfer.manager import OmicsTransferConfig, OmicsTransferManager
from tests import (
    TEST_CONSTANTS,
    TEST_CONSTANTS_REFERENCE_STORE,
    RecordingSubscriber,
    StreamWithError,
    StubbedClientTest,
)
from tests.functional import (
    add_get_readset_metadata_response,
    add_get_readset_responses,
    add_get_reference_metadata_response,
    add_get_reference_responses,
    create_download_readset_call_kwargs,
    create_download_reference_call_kwargs,
)


class BaseDownloadTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.config = OmicsTransferConfig(max_request_concurrency=1)
        self._manager = OmicsTransferManager(self.client, self.config)
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")

    @property
    def manager(self):
        return self._manager

    def add_n_retryable_get_readset_responses(self, n, num_reads=0):
        for _ in range(n):
            self.stubber.add_response(
                method="get_read_set",
                service_response={
                    "payload": StreamWithError(
                        copy.deepcopy(BytesIO(TEST_CONSTANTS["content"])), SOCKET_ERROR, num_reads
                    )
                },
            )

    def add_n_retryable_get_reference_responses(self, n, num_reads=0):
        for _ in range(n):
            self.stubber.add_response(
                method="get_reference",
                service_response={
                    "payload": StreamWithError(
                        copy.deepcopy(BytesIO(TEST_CONSTANTS_REFERENCE_STORE["content"])),
                        SOCKET_ERROR,
                        num_reads,
                    )
                },
            )

    def test_download_readset_temporary_file_does_not_exist(self):
        add_get_readset_metadata_response(self.stubber)
        add_get_readset_responses(self.stubber)

        future = self.manager.download_readset(**create_download_readset_call_kwargs(self.filename))
        future.result()

        self.assertTrue(os.path.exists(self.filename))
        possible_matches = glob.glob("'%s*'" % self.filename + os.extsep)
        self.assertEqual(possible_matches, [])

    def test_download_readset_for_fileobj(self):
        add_get_readset_metadata_response(self.stubber)
        add_get_readset_responses(self.stubber)

        with open(self.filename, "wb") as f:
            future = self.manager.download_readset(
                **create_download_readset_call_kwargs(self.filename)
            )
            future.result()

        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_download_readset_for_seekable_filelike_obj(self):
        add_get_readset_metadata_response(self.stubber)
        add_get_readset_responses(self.stubber)

        # Create a file-like object to test. In this case, it is a BytesIO
        # object.
        bytes_io = BytesIO()
        future = self.manager.download_readset(
            TEST_CONSTANTS["sequence_store_id"], TEST_CONSTANTS["read_set_id"], bytes_io
        )
        future.result()

        bytes_io.seek(0)
        self.assertEqual(TEST_CONSTANTS["content"], bytes_io.read())

    def test_download_readset_cleanup_on_failure(self):
        add_get_readset_metadata_response(self.stubber)

        # Throw an error on the download
        self.stubber.add_client_error("get_read_set")

        future = self.manager.download_readset(**create_download_readset_call_kwargs(self.filename))

        with self.assertRaises(ClientError):
            future.result()
        # Make sure the actual file and the temporary do not exist
        # by globbing for the file and any of its extensions
        possible_matches = glob.glob("'%s*'" % self.filename)
        self.assertEqual(possible_matches, [])

    def test_download_readset_with_nonexistent_directory(self):
        add_get_readset_metadata_response(self.stubber)
        add_get_readset_responses(self.stubber)

        call_kwargs = create_download_readset_call_kwargs(self.filename)
        call_kwargs["fileobj"] = os.path.join(self.tempdir, "missing-directory", "test_file")
        future = self.manager.download_readset(**call_kwargs)

        with self.assertRaises(IOError):
            future.result()

    def test_download_readset_retries_and_succeeds(self):
        add_get_readset_metadata_response(self.stubber)
        # Insert a response that will trigger a retry.
        self.add_n_retryable_get_readset_responses(1)
        # Add the normal responses to simulate the download proceeding
        # as normal after the retry.
        add_get_readset_responses(self.stubber)

        future = self.manager.download_readset(**create_download_readset_call_kwargs(self.filename))
        future.result()

        # The retry should have been consumed and the process should have
        # continued using the successful responses.
        self.stubber.assert_no_pending_responses()
        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS["content"], f.read())

    def test_download_readset_retry_failure(self):
        add_get_readset_metadata_response(self.stubber)

        max_retries = 3
        self.config.num_download_attempts = max_retries
        self._manager = OmicsTransferManager(self.client, self.config)
        # Add responses that fill up the maximum number of retries.
        self.add_n_retryable_get_readset_responses(max_retries)

        future = self.manager.download_readset(**create_download_readset_call_kwargs(self.filename))

        with self.assertRaises(RetriesExceededError):
            future.result()
        self.stubber.assert_no_pending_responses()

    def test_download_readset_if_file_metadata_provided_get_readset_metadata_not_called(self):
        add_get_readset_responses(self.stubber)

        call_kwargs = create_download_readset_call_kwargs(self.filename)
        call_kwargs["file_metadata"] = {
            "part_size": TEST_CONSTANTS["part_size"],
            "total_parts": TEST_CONSTANTS["total_parts"],
            "content_length": len(TEST_CONSTANTS["content"]),
        }

        future = self.manager.download_readset(**call_kwargs)
        future.result()

        self.stubber.assert_no_pending_responses()

    def test_download_readset_retry_rewinds_callbacks(self):
        add_get_readset_metadata_response(self.stubber)
        # Insert a response that will trigger a retry after one read of the
        # stream has been made.
        self.add_n_retryable_get_readset_responses(1, num_reads=1)

        # Add the normal responses to simulate the download proceeding
        # as normal after the retry.
        add_get_readset_responses(self.stubber)

        recorder_subscriber = RecordingSubscriber()
        # Set the streaming to a size that is smaller than the data we
        # currently provide to it to simulate rewinds of callbacks.
        self.config.io_chunksize = 1
        future = self.manager.download_readset(
            **create_download_readset_call_kwargs(self.filename), subscribers=[recorder_subscriber]
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

    def test_download_reference_temporary_file_does_not_exist(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        future = self.manager.download_reference(
            **create_download_reference_call_kwargs(self.filename)
        )
        future.result()

        self.assertTrue(os.path.exists(self.filename))
        possible_matches = glob.glob("'%s*'" % self.filename + os.extsep)
        self.assertEqual(possible_matches, [])

    def test_download_reference_for_fileobj(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        with open(self.filename, "wb") as f:
            future = self.manager.download_reference(
                **create_download_reference_call_kwargs(self.filename)
            )
            future.result()

        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())

    def test_download_reference_for_seekable_filelike_obj(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        # Create a file-like object to test. In this case, it is a BytesIO
        # object.
        bytes_io = BytesIO()
        future = self.manager.download_reference(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            bytes_io,
        )
        future.result()

        bytes_io.seek(0)
        self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], bytes_io.read())

    def test_download_reference_cleanup_on_failure(self):
        add_get_reference_metadata_response(self.stubber)

        # Throw an error on the download
        self.stubber.add_client_error("get_reference")

        future = self.manager.download_reference(
            **create_download_reference_call_kwargs(self.filename)
        )

        with self.assertRaises(ClientError):
            future.result()
        # Make sure the actual file and the temporary do not exist
        # by globbing for the file and any of its extensions
        possible_matches = glob.glob("'%s*'" % self.filename)
        self.assertEqual(possible_matches, [])

    def test_download_reference_with_nonexistent_directory(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        call_kwargs = create_download_reference_call_kwargs(self.filename)
        call_kwargs["fileobj"] = os.path.join(self.tempdir, "missing-directory", "test_file")
        future = self.manager.download_reference(**call_kwargs)

        with self.assertRaises(IOError):
            future.result()

    def test_download_reference_retries_and_succeeds(self):
        add_get_reference_metadata_response(self.stubber)
        # Insert a response that will trigger a retry.
        self.add_n_retryable_get_reference_responses(1)
        # Add the normal responses to simulate the download proceeding
        # as normal after the retry.
        add_get_reference_responses(self.stubber)

        future = self.manager.download_reference(
            **create_download_reference_call_kwargs(self.filename)
        )
        future.result()

        # The retry should have been consumed and the process should have
        # continued using the successful responses.
        self.stubber.assert_no_pending_responses()
        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())

    def test_download_reference_retry_failure(self):
        add_get_reference_metadata_response(self.stubber)

        max_retries = 3
        self.config.num_download_attempts = max_retries
        self._manager = OmicsTransferManager(self.client, self.config)
        # Add responses that fill up the maximum number of retries.
        self.add_n_retryable_get_reference_responses(max_retries)

        future = self.manager.download_reference(
            **create_download_reference_call_kwargs(self.filename)
        )

        with self.assertRaises(RetriesExceededError):
            future.result()
        self.stubber.assert_no_pending_responses()

    def test_download_reference_if_file_metadata_provided_get_reference_metadata_not_called(self):
        add_get_reference_responses(self.stubber)

        call_kwargs = create_download_reference_call_kwargs(self.filename)
        call_kwargs["file_metadata"] = {
            "part_size": TEST_CONSTANTS_REFERENCE_STORE["part_size"],
            "total_parts": TEST_CONSTANTS_REFERENCE_STORE["total_parts"],
            "content_length": len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
        }

        future = self.manager.download_reference(**call_kwargs)
        future.result()

        self.stubber.assert_no_pending_responses()

    def test_download_reference_retry_rewinds_callbacks(self):
        add_get_reference_metadata_response(self.stubber)
        # Insert a response that will trigger a retry after one read of the
        # stream has been made.
        self.add_n_retryable_get_reference_responses(1, num_reads=1)

        # Add the normal responses to simulate the download proceeding
        # as normal after the retry.
        add_get_reference_responses(self.stubber)

        recorder_subscriber = RecordingSubscriber()
        # Set the streaming to a size that is smaller than the data we
        # currently provide to it to simulate rewinds of callbacks.
        self.config.io_chunksize = 1
        future = self.manager.download_reference(
            **create_download_reference_call_kwargs(self.filename),
            subscribers=[recorder_subscriber]
        )
        future.result()

        self.stubber.assert_no_pending_responses()
        with open(self.filename, "rb") as f:
            self.assertEqual(TEST_CONSTANTS_REFERENCE_STORE["content"], f.read())

        self.assertEqual(
            recorder_subscriber.calculate_bytes_seen(),
            len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
        )

        # Also ensure that the second progress invocation was negative one
        # because a retry happened on the second read of the stream and we
        # know that the chunk size for each read is one.
        progress_byte_list = [
            call["bytes_transferred"] for call in recorder_subscriber.on_progress_calls
        ]
        self.assertEqual(-1, progress_byte_list[1])
