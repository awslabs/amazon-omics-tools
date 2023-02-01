import unittest

import botocore.session
from botocore.stub import Stubber
from s3transfer.futures import BoundedExecutor, TransferCoordinator, TransferMeta
from s3transfer.utils import OSUtils

from omics.common.omics_file_types import ReadSetFileName, ReferenceFileName
from omics.transfer import OmicsTransferFuture, OmicsTransferSubscriber
from omics.transfer.config import TransferConfig

TEST_CONSTANTS = {
    "sequence_store_id": "1234567890",
    "read_set_id": "0987654321",
    "content": b"my test content",
    "file": ReadSetFileName.SOURCE1.value,
    "part_size": 2,
    "total_parts": 8,
}

TEST_CONSTANTS_REFERENCE_STORE = {
    "reference_store_id": "1234567890",
    "reference_id": "0987654321",
    "content": b"my ref test content",
    "file": ReferenceFileName.SOURCE.value,
    "part_size": 2,
    "total_parts": 10,
}


class StubbedClientTest(unittest.TestCase):
    def setUp(self):
        self.session = botocore.session.get_session()
        self.region = "us-west-2"
        self.client = self.session.create_client(
            "omics",
            self.region,
            aws_access_key_id="foo",
            aws_secret_access_key="bar",
        )
        self.stubber = Stubber(self.client)
        self.stubber.activate()

    def tearDown(self):
        self.stubber.deactivate()

    def reset_stubber_with_new_client(self, override_client_kwargs):
        client_kwargs = {
            "service_name": "omics",
            "region_name": self.region,
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
        }
        client_kwargs.update(override_client_kwargs)
        self.client = self.session.create_client(**client_kwargs)
        self.stubber = Stubber(self.client)
        self.stubber.activate()


class BaseTaskTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.transfer_coordinator = TransferCoordinator()

    def get_task(self, task_cls, **kwargs):
        if "transfer_coordinator" not in kwargs:
            kwargs["transfer_coordinator"] = self.transfer_coordinator
        return task_cls(**kwargs)

    def get_transfer_future(self, call_args=None):
        return OmicsTransferFuture(
            meta=TransferMeta(call_args), coordinator=self.transfer_coordinator
        )


class BaseSubmissionTaskTest(BaseTaskTest):
    def setUp(self):
        super().setUp()
        self.config = TransferConfig()
        self.osutil = OSUtils()
        self.executor = BoundedExecutor(
            1000,
            1,
        )

    def tearDown(self):
        super().tearDown()
        self.executor.shutdown()


class RecordingExecutor:
    """A wrapper on an executor to record calls made to submit().

    You can access the submissions property to receive a list of dictionaries
    that represents all submissions where the dictionary is formatted::

        {
            'fn': function
            'args': positional args (as tuple)
            'kwargs': keyword args (as dict)
        }
    """

    def __init__(self, executor):
        self._executor = executor
        self.submissions = []

    def submit(self, task, tag=None, block=True):
        future = self._executor.submit(task, tag, block)
        self.submissions.append({"task": task, "tag": tag, "block": block})
        return future

    def shutdown(self):
        self._executor.shutdown()


class StreamWithError:
    """A wrapper to simulate errors while reading from a stream.

    :param stream: The underlying stream to read from
    :param exception_type: The exception type to throw
    :param num_reads: The number of times to allow a read before raising
        the exception. A value of zero indicates to raise the error on the
        first read.
    """

    def __init__(self, stream, exception_type, num_reads=0):
        self._stream = stream
        self._exception_type = exception_type
        self._num_reads = num_reads
        self._count = 0

    def read(self, n=-1):
        if self._count == self._num_reads:
            raise self._exception_type
        self._count += 1
        return self._stream.read(n)


class RecordingSubscriber(OmicsTransferSubscriber):
    def __init__(self):
        self.on_queued_calls = []
        self.on_progress_calls = []
        self.on_done_calls = []

    def on_queued(self, **kwargs):
        self.on_queued_calls.append(kwargs)

    def on_progress(self, **kwargs):
        self.on_progress_calls.append(kwargs)

    def on_done(self, **kwargs):
        self.on_done_calls.append(kwargs)

    def calculate_bytes_seen(self, **kwargs):
        amount_seen = 0
        for call in self.on_progress_calls:
            amount_seen += call["bytes_transferred"]
        return amount_seen
