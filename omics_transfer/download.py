# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import socket
from typing import IO, Any, Callable, List, Type, Union

from botocore.exceptions import IncompleteReadError, ReadTimeoutError
from mypy_boto3_omics.client import OmicsClient
from s3transfer.download import (
    DownloadChunkIterator,
    DownloadFilenameOutputManager,
    DownloadOutputManager,
    DownloadSeekableOutputManager,
)
from s3transfer.futures import TransferFuture
from s3transfer.tasks import SubmissionTask, Task
from s3transfer.utils import (
    CountCallbackInvoker,
    FunctionContainer,
    OSUtils,
    StreamReaderProgress,
    get_callbacks,
    invoke_progress_callbacks,
)

from omics_transfer import RetriesExceededError
from omics_transfer.config import OmicsTransferConfig
from omics_transfer.utils import check_required_file_metadata_present

logger = logging.getLogger(__name__)

# In python 3, all the socket related errors are in a newly created ConnectionError.
SOCKET_ERROR = ConnectionError
RETRYABLE_DOWNLOAD_ERRORS = (socket.timeout, ConnectionError, IncompleteReadError, ReadTimeoutError)


class OmicsDownloadSubmissionTask(SubmissionTask):
    """Base class for managing Omics download tasks."""

    def _get_download_output_manager_cls(
        self, transfer_future: TransferFuture, osutil: OSUtils
    ) -> Type[DownloadOutputManager]:
        """Return the type of download submission class to use based on the fileobj of the call.

        Args:
            transfer_future: The transfer future for the request
            osutil: The os utility associated to the transfer

        Returns:
            The appropriate class to use for managing a specific type of input for downloads.

        Raises:
            RuntimeError: if there are no available download classes for the given fileobj.
        """
        download_manager_resolver_chain = [
            DownloadFilenameOutputManager,
            DownloadSeekableOutputManager,
        ]
        fileobj = transfer_future.meta.call_args.fileobj
        for download_manager_cls in download_manager_resolver_chain:
            if download_manager_cls.is_compatible(fileobj, osutil):
                return download_manager_cls

        raise RuntimeError("Output {} of type: {} is not supported.".format(fileobj, type(fileobj)))

    def _submit(self, transfer_future: TransferFuture, **kwargs: dict) -> None:
        raise NotImplementedError("_submit()")

    def _get_final_io_task_submission_callback(
        self, download_manager: DownloadOutputManager, io_executor: Any
    ) -> FunctionContainer:
        final_task = download_manager.get_final_io_task()
        return FunctionContainer(self._transfer_coordinator.submit, io_executor, final_task)


class ReadSetDownloadSubmissionTask(OmicsDownloadSubmissionTask):
    """Manages a read set download task."""

    # Ignoring incompatibility with base class definition until mypy supports typed **kwargs.
    def _submit(  # type:ignore
        self,
        transfer_future: TransferFuture,
        client: OmicsClient,
        config: OmicsTransferConfig,
        osutil: OSUtils,
        request_executor: Callable[..., Any],
        io_executor: Any,
    ) -> None:
        download_output_manager = self._get_download_output_manager_cls(transfer_future, osutil)(
            osutil, self._transfer_coordinator, io_executor
        )

        self._submit_readset_part_download_request(
            client,
            config,
            request_executor,
            io_executor,
            download_output_manager,
            transfer_future,
        )

    def _submit_readset_part_download_request(
        self,
        client: OmicsClient,
        config: OmicsTransferConfig,
        request_executor: Callable[..., Any],
        io_executor: Any,
        download_output_manager: DownloadOutputManager,
        transfer_future: TransferFuture,
    ) -> None:
        call_args = transfer_future.meta.call_args

        # Get the needed progress callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, "progress")

        # Get a handle to the file that will be used for writing downloaded
        # contents
        fileobj = download_output_manager.get_fileobj_for_io_writes(transfer_future)

        # If necessary file metadata is present, avoid calling GetReadSetMetadata API
        # Determine the number of parts
        if check_required_file_metadata_present(call_args):
            part_size = call_args.file_metadata["part_size"]
            num_parts = call_args.file_metadata["total_parts"]
            content_length = call_args.file_metadata["content_length"]
        else:
            metadata_response = client.get_read_set_metadata(
                sequenceStoreId=call_args.sequence_store_id, id=call_args.readset_id
            )

            part_size = metadata_response["files"][call_args.file.lower()]["partSize"]
            num_parts = metadata_response["files"][call_args.file.lower()]["totalParts"]
            content_length = metadata_response["files"][call_args.file.lower()]["contentLength"]

        transfer_future.meta.provide_transfer_size(content_length)
        # Get any associated tags for the get object task.
        get_object_tag = download_output_manager.get_download_task_tag()

        # Callback invoker to submit the final io task once all downloads
        # are complete.
        finalize_download_invoker = CountCallbackInvoker(
            self._get_final_io_task_submission_callback(download_output_manager, io_executor)
        )

        for i in range(num_parts):
            finalize_download_invoker.increment()
            self._transfer_coordinator.submit(
                request_executor,
                GetReadSetTask(
                    transfer_coordinator=self._transfer_coordinator,
                    main_kwargs={
                        "client": client,
                        "sequence_store_id": call_args.sequence_store_id,
                        "readset_id": call_args.readset_id,
                        "part_number": i + 1,
                        "fileobj": fileobj,
                        "file": call_args.file,
                        "callbacks": progress_callbacks,
                        "max_attempts": config.num_download_attempts,
                        "start_index": i * part_size,
                        "download_output_manager": download_output_manager,
                        "io_chunksize": config.io_chunksize,
                    },
                    done_callbacks=[finalize_download_invoker.decrement],
                ),
                tag=get_object_tag,
            )
        finalize_download_invoker.finalize()


class GetReadSetTask(Task):
    """Manages a get read set operation."""

    def _main(
        self,
        client: OmicsClient,
        sequence_store_id: str,
        readset_id: str,
        part_number: int,
        fileobj: Union[IO[Any], str, bytes],
        file: str,
        callbacks: List[Callable[..., Any]],
        max_attempts: int,
        download_output_manager: DownloadOutputManager,
        io_chunksize: int,
        start_index: int = 0,
    ) -> None:
        last_exception = None
        for i in range(max_attempts):
            current_index = start_index
            try:
                response = client.get_read_set(
                    sequenceStoreId=sequence_store_id,
                    id=readset_id,
                    partNumber=part_number,
                    file=file,
                )
                streaming_body = StreamReaderProgress(response["payload"], callbacks)
                chunks = DownloadChunkIterator(streaming_body, io_chunksize)
                for chunk in chunks:
                    # If the transfer is done because of a cancellation
                    # or error somewhere else, stop trying to submit more
                    # data to be written and break out of the download.
                    if not self._transfer_coordinator.done():
                        download_output_manager.queue_file_io_task(fileobj, chunk, current_index)
                        current_index += len(chunk)
                    else:
                        return
                return
            except RETRYABLE_DOWNLOAD_ERRORS as e:
                logger.debug(
                    "Retrying exception caught (%s), " "retrying request, (attempt %s / %s)",
                    e,
                    i,
                    max_attempts,
                    exc_info=True,
                )
                last_exception = e
                # Also invoke the progress callbacks to indicate that we
                # are trying to download the stream again and all progress
                # for this GetObject has been lost.
                invoke_progress_callbacks(callbacks, start_index - current_index)
                continue
        raise RetriesExceededError(last_exception)


class ReferenceDownloadSubmissionTask(OmicsDownloadSubmissionTask):
    """Manages a reference download task."""

    # Ignoring incompatibility with base class definition until mypy supports typed **kwargs
    def _submit(  # type:ignore
        self,
        transfer_future: TransferFuture,
        client: OmicsClient,
        config: OmicsTransferConfig,
        osutil: OSUtils,
        request_executor: Callable[..., Any],
        io_executor: Any,
    ) -> None:
        download_output_manager = self._get_download_output_manager_cls(transfer_future, osutil)(
            osutil, self._transfer_coordinator, io_executor
        )

        self._submit_reference_part_download_request(
            client,
            config,
            request_executor,
            io_executor,
            download_output_manager,
            transfer_future,
        )

    def _submit_reference_part_download_request(
        self,
        client: OmicsClient,
        config: OmicsTransferConfig,
        request_executor: Callable[..., Any],
        io_executor: Any,
        download_output_manager: DownloadOutputManager,
        transfer_future: TransferFuture,
    ) -> None:
        call_args = transfer_future.meta.call_args

        # Get the needed progress callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, "progress")

        # Get a handle to the file that will be used for writing downloaded
        # contents
        fileobj = download_output_manager.get_fileobj_for_io_writes(transfer_future)

        # If necessary file metadata is present, avoid calling GetReadSetMetadata API
        # Determine the number of parts
        if check_required_file_metadata_present(call_args):
            part_size = call_args.file_metadata["part_size"]
            num_parts = call_args.file_metadata["total_parts"]
            content_length = call_args.file_metadata["content_length"]
        else:
            metadata_response = client.get_reference_metadata(
                referenceStoreId=call_args.reference_store_id, id=call_args.reference_id
            )

            part_size = metadata_response["files"][call_args.file.lower()]["partSize"]
            num_parts = metadata_response["files"][call_args.file.lower()]["totalParts"]
            content_length = metadata_response["files"][call_args.file.lower()]["contentLength"]

        transfer_future.meta.provide_transfer_size(content_length)
        # Get any associated tags for the get object task.
        get_object_tag = download_output_manager.get_download_task_tag()

        # Callback invoker to submit the final io task once all downloads
        # are complete.
        finalize_download_invoker = CountCallbackInvoker(
            self._get_final_io_task_submission_callback(download_output_manager, io_executor)
        )

        for i in range(num_parts):
            finalize_download_invoker.increment()
            self._transfer_coordinator.submit(
                request_executor,
                GetReferenceTask(
                    transfer_coordinator=self._transfer_coordinator,
                    main_kwargs={
                        "client": client,
                        "reference_store_id": call_args.reference_store_id,
                        "reference_id": call_args.reference_id,
                        "part_number": i + 1,
                        "fileobj": fileobj,
                        "file": call_args.file,
                        "callbacks": progress_callbacks,
                        "max_attempts": config.num_download_attempts,
                        "start_index": i * part_size,
                        "download_output_manager": download_output_manager,
                        "io_chunksize": config.io_chunksize,
                    },
                    done_callbacks=[finalize_download_invoker.decrement],
                ),
                tag=get_object_tag,
            )
        finalize_download_invoker.finalize()


class GetReferenceTask(Task):
    """Manages a get reference operation."""

    def _main(
        self,
        client: OmicsClient,
        reference_store_id: str,
        reference_id: str,
        part_number: int,
        fileobj: Union[IO[Any], str, bytes],
        file: str,
        callbacks: List[Callable[..., Any]],
        max_attempts: int,
        download_output_manager: DownloadOutputManager,
        io_chunksize: int,
        start_index: int = 0,
    ) -> None:
        last_exception = None
        for i in range(max_attempts):
            current_index = start_index
            try:
                response = client.get_reference(
                    referenceStoreId=reference_store_id,
                    id=reference_id,
                    partNumber=part_number,
                    file=file,
                )
                streaming_body = StreamReaderProgress(response["payload"], callbacks)
                chunks = DownloadChunkIterator(streaming_body, io_chunksize)
                for chunk in chunks:
                    # If the transfer is done because of a cancellation
                    # or error somewhere else, stop trying to submit more
                    # data to be written and break out of the download.
                    if not self._transfer_coordinator.done():
                        download_output_manager.queue_file_io_task(fileobj, chunk, current_index)
                        current_index += len(chunk)
                    else:
                        return
                return
            except RETRYABLE_DOWNLOAD_ERRORS as e:
                logger.debug(
                    "Retrying exception caught (%s), " "retrying request, (attempt %s / %s)",
                    e,
                    i,
                    max_attempts,
                    exc_info=True,
                )
                last_exception = e
                # Also invoke the progress callbacks to indicate that we
                # are trying to download the stream again and all progress
                # for this GetObject has been lost.
                invoke_progress_callbacks(callbacks, start_index - current_index)
                continue
        raise RetriesExceededError(last_exception)
