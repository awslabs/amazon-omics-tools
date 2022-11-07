# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from s3transfer.constants import KB


class OmicsTransferConfig:
    """Configuration options for Omics Transfer."""

    def __init__(
        self,
        max_request_concurrency=10,
        max_submission_concurrency=5,
        max_request_queue_size=1000,
        max_submission_queue_size=1000,
        max_io_queue_size=1000,
        io_chunksize=256 * KB,
        num_download_attempts=5,
    ):
        """Initialize an OmicsTransferConfig object.

        Args:
            max_request_concurrency: The maximum number of Omics API
                transfer-related requests that can happen at a time.

            max_submission_concurrency: The maximum number of threads
                processing a call to a TransferManager method. Processing a
                call usually entails determining which Omics API requests need
                to be enqueued, but does **not** entail making any of the
                Omics API data transferring requests needed to perform the transfer.
                The threads controlled by ``max_request_concurrency`` is
                responsible for that.

            max_request_queue_size: The maximum amount of Omics API requests
                that can be queued at a time.

            max_submission_queue_size: The maximum amount of
                TransferManager method calls that can be queued at a time.

            max_io_queue_size: The maximum amount of read parts that
                can be queued to be written to disk per download. The default
                size for each element in this queue is 8 KB.

            io_chunksize: The max size of each chunk in the io queue.

            num_download_attempts: The number of download attempts that
                will be tried upon errors with downloading an object in Omics. Note
                that these retries account for errors that occur when streaming
                down the data from Omics (i.e. socket errors and read timeouts that
                occur after receiving an OK response from Omics).
                Other retryable exceptions such as throttling errors and 5xx errors
                are already retried by botocore (this default is 5). The
                ``num_download_attempts`` does not take into account the
                number of exceptions retried by botocore.
        """
        self.max_request_concurrency = max_request_concurrency
        self.max_submission_concurrency = max_submission_concurrency
        self.max_request_queue_size = max_request_queue_size
        self.max_submission_queue_size = max_submission_queue_size
        self.max_io_queue_size = max_io_queue_size
        self.io_chunksize = io_chunksize
        self.num_download_attempts = num_download_attempts
        self._validate_attrs_are_nonzero()

    def _validate_attrs_are_nonzero(self):
        for attr, attr_val in self.__dict__.items():
            if attr_val is not None and attr_val <= 0:
                raise ValueError(
                    "Provided parameter %s of value %s must be greater than "
                    "0." % (attr, attr_val)
                )
