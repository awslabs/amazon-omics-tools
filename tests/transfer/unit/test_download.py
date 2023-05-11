import copy
import os.path
import shutil
import tempfile
from io import BytesIO
from typing import IO, Any, Tuple, Union

from botocore.stub import ANY
from s3transfer.download import DownloadSeekableOutputManager
from s3transfer.exceptions import RetriesExceededError
from s3transfer.futures import BoundedExecutor, TransferMeta
from s3transfer.utils import OSUtils

from omics.common.omics_file_types import OmicsFileType
from omics.transfer import FileDownload, OmicsTransferFuture
from omics.transfer.download import (
    SOCKET_ERROR,
    DownloadSubmissionTask,
    GetFileTask,
    OmicsDownloadFilenameOutputManager,
)
from tests.transfer import (
    TEST_CONSTANTS,
    TEST_CONSTANTS_REFERENCE_STORE,
    BaseSubmissionTaskTest,
    BaseTaskTest,
    RecordingExecutor,
    StreamWithError,
)
from tests.transfer.functional import (
    add_get_read_set_metadata_response,
    add_get_read_set_responses,
)


def get_expected_read_set_api_call_params():
    return {
        "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
        "id": TEST_CONSTANTS["read_set_id"],
        "partNumber": ANY,
        "file": TEST_CONSTANTS["file"],
    }


def get_expected_reference_api_call_params():
    return {
        "referenceStoreId": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
        "id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
        "partNumber": ANY,
        "file": TEST_CONSTANTS_REFERENCE_STORE["file"],
    }


class WriteCollector:
    """A utility to collect information about writes and seeks."""

    def __init__(self):
        self._pos = 0
        self.writes = []

    def seek(self, pos, whence=0):
        self._pos = pos

    def write(self, data):
        self.writes.append((self._pos, data))
        self._pos += len(data)


class CancelledStreamWrapper:
    """A wrapper to trigger a cancellation while stream reading.

    Forces the transfer coordinator to cancel after a certain amount of reads.

    Args:
        stream: The underlying stream to read from.
        transfer_coordinator: The coordinator for the transfer.
        num_reads: On which read to signal a cancellation. 0 is the first read.
    """

    def __init__(self, stream, transfer_coordinator, num_reads=0):
        self._stream = stream
        self._transfer_coordinator = transfer_coordinator
        self._num_reads = num_reads
        self._count = 0

    def read(self, *args, **kwargs):
        if self._num_reads == self._count:
            self._transfer_coordinator.cancel()
        self._stream.read(*args, **kwargs)
        self._count += 1


class TestDownloadSubmissionTask(BaseSubmissionTaskTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self.subscribers = []

        self.call_args = self.get_call_args(OmicsFileType.READSET)
        self.transfer_future = self.get_transfer_future(self.call_args)
        self.omics_download_submission_task = DownloadSubmissionTask(self.transfer_coordinator)

        self.io_executor = BoundedExecutor(1000, 1)
        self.download_manager = OmicsDownloadFilenameOutputManager(
            self.osutil, self.transfer_coordinator, self.io_executor
        )

        self.submission_main_kwargs = {
            "transfer_future": self.transfer_future,
            "client": self.client,
            "config": self.config,
            "request_executor": self.executor,
            "download_manager": self.download_manager,
            "io_executor": self.io_executor,
        }
        self.submission_task = self.get_task(
            DownloadSubmissionTask, main_kwargs=self.submission_main_kwargs
        )

    def tearDown(self):
        super().tearDown()
        self.io_executor.shutdown()
        shutil.rmtree(self.tempdir)

    def get_call_args(
        self, file_type: OmicsFileType, fileobj: Union[IO[Any], str] = None
    ) -> FileDownload:
        if file_type == OmicsFileType.READSET:
            store_id = TEST_CONSTANTS["sequence_store_id"]
            file_set_id = TEST_CONSTANTS["read_set_id"]
        elif file_type == OmicsFileType.REFERENCE:
            store_id = TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"]
            file_set_id = TEST_CONSTANTS_REFERENCE_STORE["reference_id"]

        if fileobj is None:
            # The default fileobj is the test filename
            fileobj = self.filename

        return FileDownload(
            store_id=store_id,
            file_set_id=file_set_id,
            filename=TEST_CONSTANTS["file"],
            fileobj=fileobj,
            omics_file_type=file_type,
            subscribers=self.subscribers,
        )

    def init_submission_task(
        self, call_args: FileDownload
    ) -> Tuple[OmicsTransferFuture, DownloadSubmissionTask]:
        transfer_future = OmicsTransferFuture(
            meta=TransferMeta(call_args), coordinator=self.transfer_coordinator
        )
        task_kwargs = {
            "transfer_future": transfer_future,
            "client": self.client,
            "config": self.config,
            "request_executor": self.executor,
            "download_manager": self.download_manager,
            "io_executor": self.io_executor,
        }
        submission_task = DownloadSubmissionTask(
            transfer_coordinator=self.transfer_coordinator, main_kwargs=task_kwargs
        )
        return transfer_future, submission_task

    def wait_and_assert_completed_successfully(self, submission_task):
        submission_task()
        self.transfer_future.result()
        self.stubber.assert_no_pending_responses()

    def wrap_executor_in_recorder(self):
        self.executor = RecordingExecutor(self.executor)
        self.submission_main_kwargs["request_executor"] = self.executor

    def use_fileobj_in_call_args(self, fileobj):
        self.call_args = self.get_call_args(OmicsFileType.READSET, fileobj)
        self.transfer_future = self.get_transfer_future(self.call_args)
        self.submission_main_kwargs["transfer_future"] = self.transfer_future

    def test_submit(self):
        self.wrap_executor_in_recorder()
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        self.submission_main_kwargs["download_manager"] = OmicsDownloadFilenameOutputManager(
            self.osutil, self.transfer_coordinator, self.io_executor
        )

        self.submission_task = self.get_task(
            DownloadSubmissionTask, main_kwargs=self.submission_main_kwargs
        )
        self.wait_and_assert_completed_successfully(self.submission_task)
        assert len(self.executor.submissions) == TEST_CONSTANTS["total_parts"]
        assert self.osutil.get_file_size(self.filename) == len(TEST_CONSTANTS["content"])

    def test_submit_with_seekable_file_object(self):
        self.wrap_executor_in_recorder()
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        self.submission_main_kwargs["download_manager"] = DownloadSeekableOutputManager(
            self.osutil, self.transfer_coordinator, self.io_executor
        )

        with open(self.filename, "wb") as f:
            self.use_fileobj_in_call_args(f)
            self.submission_task = self.get_task(
                DownloadSubmissionTask, main_kwargs=self.submission_main_kwargs
            )
            self.wait_and_assert_completed_successfully(self.submission_task)

        assert len(self.executor.submissions) == TEST_CONSTANTS["total_parts"]
        assert self.osutil.get_file_size(self.filename) == len(TEST_CONSTANTS["content"])

    def test_submit_with_nonexistent_file(self):
        add_get_read_set_metadata_response(self.stubber)
        call_args = FileDownload(
            store_id="mock-store-id",
            file_set_id="mock-file-set-id",
            filename="nonexistent-file",
            fileobj="mock-fileobj",
            omics_file_type=OmicsFileType.READSET,
        )

        transfer_future, submission_task = self.init_submission_task(call_args)
        submission_task()
        with self.assertRaises(ValueError):
            transfer_future.result()

    def test_submit_with_invalid_file_type(self):
        call_args = FileDownload(
            store_id="mock-store-id",
            file_set_id="mock-file-set-id",
            filename=TEST_CONSTANTS["file"],
            fileobj="mock-fileobj",
            omics_file_type="INVALID_FILE_TYPE",
        )

        transfer_future, submission_task = self.init_submission_task(call_args)
        submission_task()
        with self.assertRaises(AttributeError):
            transfer_future.result()


class TestGetFileTask(BaseTaskTest):
    def setUp(self):
        super().setUp()
        self.part_number = 1
        self.callbacks = []
        self.max_attempts = 5
        self.io_executor = BoundedExecutor(1000, 1)
        self.stream = BytesIO(TEST_CONSTANTS["content"])
        self.fileobj = WriteCollector()
        self.osutil = OSUtils()
        self.io_chunksize = 256 * (1024**2)
        self.task_cls = GetFileTask
        self.download_output_manager = OmicsDownloadFilenameOutputManager(
            self.osutil, self.transfer_coordinator, self.io_executor
        )

    def get_download_task(self, **kwargs):
        default_kwargs = {
            "client": self.client,
            "omics_file_type": OmicsFileType.READSET,
            "store_id": TEST_CONSTANTS["sequence_store_id"],
            "file_set_id": TEST_CONSTANTS["read_set_id"],
            "part_number": self.part_number,
            "file": TEST_CONSTANTS["file"],
            "fileobj": self.fileobj,
            "callbacks": self.callbacks,
            "max_attempts": self.max_attempts,
            "download_output_manager": self.download_output_manager,
            "io_chunksize": self.io_chunksize,
        }
        default_kwargs.update(kwargs)
        self.transfer_coordinator.set_status_to_queued()
        return self.get_task(self.task_cls, main_kwargs=default_kwargs)

    def assert_io_writes(self, expected_writes):
        # Let the io executor process all of the writes before checking
        # what writes were sent to it.
        self.io_executor.shutdown()
        self.assertEqual(self.fileobj.writes, expected_writes)

    def test_main(self):
        self.stubber.add_response(
            "get_read_set",
            service_response={"payload": self.stream},
            expected_params=get_expected_read_set_api_call_params(),
        )
        task = self.get_download_task()
        task()
        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(0, TEST_CONSTANTS["content"])])

    def test_control_chunk_size(self):
        self.stubber.add_response(
            "get_read_set",
            service_response={"payload": self.stream},
            expected_params=get_expected_read_set_api_call_params(),
        )
        task = self.get_download_task(io_chunksize=1)
        task()

        self.stubber.assert_no_pending_responses()
        expected_contents = []
        for i in range(len(TEST_CONSTANTS["content"])):
            expected_contents.append((i, bytes(TEST_CONSTANTS["content"][i : i + 1])))
        self.assert_io_writes(expected_contents)

    def test_start_index(self):
        self.stubber.add_response(
            "get_read_set",
            service_response={"payload": self.stream},
            expected_params=get_expected_read_set_api_call_params(),
        )
        task = self.get_download_task(start_index=5)
        task()

        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(5, TEST_CONSTANTS["content"])])

    def test_retries_succeeds(self):
        self.stubber.add_response(
            "get_read_set",
            service_response={"payload": StreamWithError(self.stream, SOCKET_ERROR)},
            expected_params=get_expected_read_set_api_call_params(),
        )
        self.stubber.add_response(
            "get_read_set",
            service_response={"payload": self.stream},
            expected_params=get_expected_read_set_api_call_params(),
        )
        task = self.get_download_task()
        task()

        # Retryable error should have not affected the bytes placed into
        # the io queue.
        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(0, TEST_CONSTANTS["content"])])

    def test_retries_failure(self):
        for _ in range(self.max_attempts):
            self.stubber.add_response(
                "get_read_set",
                service_response={"payload": StreamWithError(self.stream, SOCKET_ERROR)},
                expected_params=get_expected_read_set_api_call_params(),
            )

        task = self.get_download_task()
        task()
        self.transfer_coordinator.announce_done()

        # Should have failed out on a RetriesExceededError
        with self.assertRaises(RetriesExceededError):
            self.transfer_coordinator.result()
        self.stubber.assert_no_pending_responses()

    def test_retries_in_middle_of_streaming(self):
        # After the first read a retryable error will be thrown
        self.stubber.add_response(
            "get_read_set",
            service_response={
                "payload": StreamWithError(copy.deepcopy(self.stream), SOCKET_ERROR, 1)
            },
            expected_params=get_expected_read_set_api_call_params(),
        )
        self.stubber.add_response(
            "get_read_set",
            service_response={"payload": self.stream},
            expected_params=get_expected_read_set_api_call_params(),
        )
        task = self.get_download_task(io_chunksize=1)
        task()

        self.stubber.assert_no_pending_responses()
        # This is the content initially read in before the retry hit on the
        # second read()
        expected_contents = [(0, bytes(TEST_CONSTANTS["content"][0:1]))]

        # The rest of the content should be the entire set of data partitioned
        # out based on the one byte stream chunk size. Note the second
        # element in the list should be a copy of the first element since
        # a retryable exception happened in between.
        for i in range(len(TEST_CONSTANTS["content"])):
            expected_contents.append((i, bytes(TEST_CONSTANTS["content"][i : i + 1])))
        self.assert_io_writes(expected_contents)

    def test_cancels_out_of_queueing(self):
        self.stubber.add_response(
            "get_read_set",
            service_response={
                "payload": CancelledStreamWrapper(self.stream, self.transfer_coordinator)
            },
            expected_params=get_expected_read_set_api_call_params(),
        )
        task = self.get_download_task()
        task()

        self.stubber.assert_no_pending_responses()
        # Make sure that no contents were added to the queue because the task
        # should have been canceled before trying to add the contents to the
        # io queue.
        self.assert_io_writes([])
