import copy
import os.path
import shutil
import tempfile
from io import BytesIO

import pytest
from botocore.stub import ANY
from s3transfer.download import DownloadFilenameOutputManager
from s3transfer.futures import BoundedExecutor
from s3transfer.utils import CallArgs, OSUtils

from omics_transfer import RetriesExceededError
from omics_transfer.download import (
    SOCKET_ERROR,
    GetReadSetTask,
    GetReferenceTask,
    OmicsDownloadSubmissionTask,
    ReadSetDownloadSubmissionTask,
    ReferenceDownloadSubmissionTask,
)
from tests import (
    TEST_CONSTANTS,
    TEST_CONSTANTS_REFERENCE_STORE,
    BaseSubmissionTaskTest,
    BaseTaskTest,
    RecordingExecutor,
    StreamWithError,
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


class TestOmicsDownloadSubmissionTask(BaseSubmissionTaskTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self.subscribers = []

        self.call_args = self.get_call_args()
        self.transfer_future = self.get_transfer_future(self.call_args)
        self.omics_download_submission_task = OmicsDownloadSubmissionTask(self.transfer_coordinator)

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tempdir)

    def get_call_args(self, **kwargs):
        default_call_args = {
            "sequence_store_id": TEST_CONSTANTS["sequence_store_id"],
            "readset_id": TEST_CONSTANTS["read_set_id"],
            "fileobj": self.filename,
            "file": TEST_CONSTANTS["file"],
            "file_metadata": {
                "content_length": len(TEST_CONSTANTS["content"]),
                "part_size": TEST_CONSTANTS["part_size"],
                "total_parts": TEST_CONSTANTS["total_parts"],
            },
            "subscribers": self.subscribers,
        }
        default_call_args.update(kwargs)
        return CallArgs(**default_call_args)

    def test_output_manager_for_file(self):
        output_manager = self.omics_download_submission_task._get_download_output_manager_cls(
            self.transfer_future, self.osutil
        )
        if output_manager is not DownloadFilenameOutputManager:
            pytest.fail()

    def test_output_manager_for_invalid_file_throws_exception(self):
        transfer_future = self.transfer_future
        transfer_future.meta.call_args.fileobj = None
        self.assertRaises(
            RuntimeError,
            self.omics_download_submission_task._get_download_output_manager_cls,
            transfer_future,
            self.osutil,
        )

    def wait_and_assert_completed_successfully(self, submission_task):
        submission_task()
        self.transfer_future.result()
        self.stubber.assert_no_pending_responses()


class TestReadSetDownloadSubmissionTask(TestOmicsDownloadSubmissionTask):
    def setUp(self):
        super().setUp()
        self.io_executor = BoundedExecutor(1000, 1)
        self.submission_main_kwargs = {
            "client": self.client,
            "config": self.config,
            "osutil": self.osutil,
            "request_executor": self.executor,
            "io_executor": self.io_executor,
            "transfer_future": self.transfer_future,
        }
        self.submission_task = self.get_download_submission_task()

    def get_download_submission_task(self):
        return self.get_task(ReadSetDownloadSubmissionTask, main_kwargs=self.submission_main_kwargs)

    def wrap_executor_in_recorder(self):
        self.executor = RecordingExecutor(self.executor)
        self.submission_main_kwargs["request_executor"] = self.executor

    def add_get_readset_metadata_response(self):
        self.stubber.add_response(
            "get_read_set_metadata",
            {
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
                },
            },
        )

    def add_get_readset_responses(self):
        for i in range(0, len(TEST_CONSTANTS["content"]), TEST_CONSTANTS["part_size"]):
            if i + TEST_CONSTANTS["part_size"] > len(TEST_CONSTANTS["content"]):
                stream = BytesIO(TEST_CONSTANTS["content"][i:])
            else:
                stream = BytesIO(TEST_CONSTANTS["content"][i : i + TEST_CONSTANTS["part_size"]])
            self.stubber.add_response(
                "get_read_set",
                service_response=copy.deepcopy({"payload": stream}),
            )

    def use_fileobj_in_call_args(self, fileobj):
        self.call_args = self.get_call_args(fileobj=fileobj)
        self.transfer_future = self.get_transfer_future(self.call_args)
        self.submission_main_kwargs["transfer_future"] = self.transfer_future

    def test_submit_for_readset_part_download_request(self):
        self.wrap_executor_in_recorder()
        self.add_get_readset_responses()

        self.submission_task = self.get_download_submission_task()
        self.wait_and_assert_completed_successfully(self.submission_task)
        assert len(self.executor.submissions) == TEST_CONSTANTS["total_parts"]
        assert self.osutil.get_file_size(self.filename) == len(TEST_CONSTANTS["content"])

    def test_submit_for_readset_part_download_request_with_seekable_file_object(self):
        self.wrap_executor_in_recorder()
        self.add_get_readset_responses()
        with open(self.filename, "wb") as f:
            self.use_fileobj_in_call_args(f)
            self.submission_task = self.get_download_submission_task()
            self.wait_and_assert_completed_successfully(self.submission_task)

        assert len(self.executor.submissions) == TEST_CONSTANTS["total_parts"]
        assert self.osutil.get_file_size(self.filename) == len(TEST_CONSTANTS["content"])

    def test_submit_for_download_request_without_file_metadata_args_calls_get_readset_metadata(
        self,
    ):
        self.wrap_executor_in_recorder()
        self.add_get_readset_metadata_response()
        self.add_get_readset_responses()

        self.call_args = self.get_call_args(file_metadata={})
        self.transfer_future = self.get_transfer_future(self.call_args)
        self.submission_main_kwargs["transfer_future"] = self.transfer_future
        self.submission_task = self.get_download_submission_task()
        self.wait_and_assert_completed_successfully(self.submission_task)
        assert len(self.executor.submissions) == TEST_CONSTANTS["total_parts"]


class TestReferenceDownloadSubmissionTask(TestOmicsDownloadSubmissionTask):
    def setUp(self):
        super().setUp()
        self.io_executor = BoundedExecutor(1000, 1)
        self.submission_main_kwargs = {
            "client": self.client,
            "config": self.config,
            "osutil": self.osutil,
            "request_executor": self.executor,
            "io_executor": self.io_executor,
            "transfer_future": self.transfer_future,
        }
        self.submission_task = self.get_download_submission_task()

    def get_download_submission_task(self):
        return self.get_task(
            ReferenceDownloadSubmissionTask, main_kwargs=self.submission_main_kwargs
        )

    def wrap_executor_in_recorder(self):
        self.executor = RecordingExecutor(self.executor)
        self.submission_main_kwargs["request_executor"] = self.executor

    def get_call_args(self, **kwargs):
        default_call_args = {
            "reference_store_id": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            "reference_id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            "fileobj": self.filename,
            "file": TEST_CONSTANTS_REFERENCE_STORE["file"],
            "file_metadata": {
                "content_length": len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
                "part_size": TEST_CONSTANTS_REFERENCE_STORE["part_size"],
                "total_parts": TEST_CONSTANTS_REFERENCE_STORE["total_parts"],
            },
            "subscribers": self.subscribers,
        }
        default_call_args.update(kwargs)
        return CallArgs(**default_call_args)

    def add_get_reference_metadata_response(self):
        self.stubber.add_response(
            "get_reference_metadata",
            {
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
                },
                "md5": "eb247690435415724f20d8702e011966",
            },
        )

    def add_get_reference_responses(self):
        for i in range(
            0,
            len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
            TEST_CONSTANTS_REFERENCE_STORE["part_size"],
        ):
            if i + TEST_CONSTANTS_REFERENCE_STORE["part_size"] > len(
                TEST_CONSTANTS_REFERENCE_STORE["content"]
            ):
                stream = BytesIO(TEST_CONSTANTS_REFERENCE_STORE["content"][i:])
            else:
                stream = BytesIO(
                    TEST_CONSTANTS_REFERENCE_STORE["content"][
                        i : i + TEST_CONSTANTS_REFERENCE_STORE["part_size"]
                    ]
                )
            self.stubber.add_response(
                "get_reference",
                service_response=copy.deepcopy({"payload": stream}),
            )

    def use_fileobj_in_call_args(self, fileobj):
        self.call_args = self.get_call_args(fileobj=fileobj)
        self.transfer_future = self.get_transfer_future(self.call_args)
        self.submission_main_kwargs["transfer_future"] = self.transfer_future

    def test_submit_for_reference_part_download_request(self):
        self.wrap_executor_in_recorder()
        self.add_get_reference_responses()

        self.submission_task = self.get_download_submission_task()
        self.wait_and_assert_completed_successfully(self.submission_task)
        assert len(self.executor.submissions) == TEST_CONSTANTS_REFERENCE_STORE["total_parts"]
        assert self.osutil.get_file_size(self.filename) == len(
            TEST_CONSTANTS_REFERENCE_STORE["content"]
        )

    def test_submit_for_reference_part_download_request_with_seekable_file_object(self):
        self.wrap_executor_in_recorder()
        self.add_get_reference_responses()
        with open(self.filename, "wb") as f:
            self.use_fileobj_in_call_args(f)
            self.submission_task = self.get_download_submission_task()
            self.wait_and_assert_completed_successfully(self.submission_task)

        assert len(self.executor.submissions) == TEST_CONSTANTS_REFERENCE_STORE["total_parts"]
        assert self.osutil.get_file_size(self.filename) == len(
            TEST_CONSTANTS_REFERENCE_STORE["content"]
        )

    def test_submit_for_download_request_without_file_metadata_args_calls_get_reference_metadata(
        self,
    ):
        self.wrap_executor_in_recorder()
        self.add_get_reference_metadata_response()
        self.add_get_reference_responses()

        self.call_args = self.get_call_args(file_metadata={})
        self.transfer_future = self.get_transfer_future(self.call_args)
        self.submission_main_kwargs["transfer_future"] = self.transfer_future
        self.submission_task = self.get_download_submission_task()
        self.wait_and_assert_completed_successfully(self.submission_task)
        assert len(self.executor.submissions) == TEST_CONSTANTS_REFERENCE_STORE["total_parts"]


class TestGetReadSetTask(BaseTaskTest):
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
        self.task_cls = GetReadSetTask
        self.download_output_manager = DownloadFilenameOutputManager(
            self.osutil, self.transfer_coordinator, self.io_executor
        )

    def get_download_task(self, **kwargs):
        default_kwargs = {
            "client": self.client,
            "sequence_store_id": TEST_CONSTANTS["sequence_store_id"],
            "readset_id": TEST_CONSTANTS["read_set_id"],
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


class TestGetReferenceTask(BaseTaskTest):
    def setUp(self):
        super().setUp()
        self.part_number = 1
        self.callbacks = []
        self.max_attempts = 5
        self.io_executor = BoundedExecutor(1000, 1)
        self.stream = BytesIO(TEST_CONSTANTS_REFERENCE_STORE["content"])
        self.fileobj = WriteCollector()
        self.osutil = OSUtils()
        self.io_chunksize = 256 * (1024**2)
        self.task_cls = GetReferenceTask
        self.download_output_manager = DownloadFilenameOutputManager(
            self.osutil, self.transfer_coordinator, self.io_executor
        )

    def get_download_task(self, **kwargs):
        default_kwargs = {
            "client": self.client,
            "reference_store_id": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            "reference_id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            "part_number": self.part_number,
            "file": TEST_CONSTANTS_REFERENCE_STORE["file"],
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
            "get_reference",
            service_response={"payload": self.stream},
            expected_params=get_expected_reference_api_call_params(),
        )
        task = self.get_download_task()
        task()
        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(0, TEST_CONSTANTS_REFERENCE_STORE["content"])])

    def test_control_chunk_size(self):
        self.stubber.add_response(
            "get_reference",
            service_response={"payload": self.stream},
            expected_params=get_expected_reference_api_call_params(),
        )
        task = self.get_download_task(io_chunksize=1)
        task()

        self.stubber.assert_no_pending_responses()
        expected_contents = []
        for i in range(len(TEST_CONSTANTS_REFERENCE_STORE["content"])):
            expected_contents.append(
                (i, bytes(TEST_CONSTANTS_REFERENCE_STORE["content"][i : i + 1]))
            )
        self.assert_io_writes(expected_contents)

    def test_start_index(self):
        self.stubber.add_response(
            "get_reference",
            service_response={"payload": self.stream},
            expected_params=get_expected_reference_api_call_params(),
        )
        task = self.get_download_task(start_index=5)
        task()

        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(5, TEST_CONSTANTS_REFERENCE_STORE["content"])])

    def test_retries_succeeds(self):
        self.stubber.add_response(
            "get_reference",
            service_response={"payload": StreamWithError(self.stream, SOCKET_ERROR)},
            expected_params=get_expected_reference_api_call_params(),
        )
        self.stubber.add_response(
            "get_reference",
            service_response={"payload": self.stream},
            expected_params=get_expected_reference_api_call_params(),
        )
        task = self.get_download_task()
        task()

        # Retryable error should have not affected the bytes placed into
        # the io queue.
        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(0, TEST_CONSTANTS_REFERENCE_STORE["content"])])

    def test_retries_failure(self):
        for _ in range(self.max_attempts):
            self.stubber.add_response(
                "get_reference",
                service_response={"payload": StreamWithError(self.stream, SOCKET_ERROR)},
                expected_params=get_expected_reference_api_call_params(),
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
            "get_reference",
            service_response={
                "payload": StreamWithError(copy.deepcopy(self.stream), SOCKET_ERROR, 1)
            },
            expected_params=get_expected_reference_api_call_params(),
        )
        self.stubber.add_response(
            "get_reference",
            service_response={"payload": self.stream},
            expected_params=get_expected_reference_api_call_params(),
        )
        task = self.get_download_task(io_chunksize=1)
        task()

        self.stubber.assert_no_pending_responses()
        # This is the content initially read in before the retry hit on the
        # second read()
        expected_contents = [(0, bytes(TEST_CONSTANTS_REFERENCE_STORE["content"][0:1]))]

        # The rest of the content should be the entire set of data partitioned
        # out based on the one byte stream chunk size. Note the second
        # element in the list should be a copy of the first element since
        # a retryable exception happened in between.
        for i in range(len(TEST_CONSTANTS_REFERENCE_STORE["content"])):
            expected_contents.append(
                (i, bytes(TEST_CONSTANTS_REFERENCE_STORE["content"][i : i + 1]))
            )
        self.assert_io_writes(expected_contents)

    def test_cancels_out_of_queueing(self):
        self.stubber.add_response(
            "get_reference",
            service_response={
                "payload": CancelledStreamWrapper(self.stream, self.transfer_coordinator)
            },
            expected_params=get_expected_reference_api_call_params(),
        )
        task = self.get_download_task()
        task()

        self.stubber.assert_no_pending_responses()
        # Make sure that no contents were added to the queue because the task
        # should have been canceled before trying to add the contents to the
        # io queue.
        self.assert_io_writes([])
