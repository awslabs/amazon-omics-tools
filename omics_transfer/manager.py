# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import IO, Any, Dict, List, Tuple, Type, Union

from mypy_boto3_omics.client import OmicsClient
from s3transfer.futures import (
    BaseExecutor,
    BoundedExecutor,
    TransferCoordinator,
    TransferFuture,
    TransferMeta,
)
from s3transfer.manager import TransferCoordinatorController
from s3transfer.subscribers import BaseSubscriber
from s3transfer.utils import CallArgs, OSUtils, get_callbacks

from omics_transfer import CancelledError, FatalError
from omics_transfer.config import OmicsTransferConfig
from omics_transfer.download import (
    OmicsDownloadSubmissionTask,
    ReadSetDownloadSubmissionTask,
    ReferenceDownloadSubmissionTask,
)
from omics_transfer.utils import (
    REQUIRED_FILE_METADATA_ARGS,
    ReadSetFile,
    ReferenceFile,
    validate_all_known_args,
)


class OmicsTransferManager:
    """Task manager for Omics data transfers."""

    def __init__(
        self,
        client: OmicsClient,
        config: OmicsTransferConfig = None,
        osutil: OSUtils = None,
        executor_cls: Type[BaseExecutor] = None,
    ):
        """Initialize an OmicsTransferManager object.

        Args:
            client: Amazon Omics Service client.

            config: OmicsTransferConfig to associate specific configurations.

            osutil: OSUtils object to use for os-related behavior when
                using with transfer manager.

            executor_cls: The class of executor to use with the transfer
                manager. By default, concurrent.futures.ThreadPoolExecutor is used.
        """
        self._client = client
        self._config = OmicsTransferConfig() if config is None else config
        self._osutil = OSUtils() if osutil is None else osutil
        self._coordinator_controller = TransferCoordinatorController()
        # A counter to create unique id's for each transfer submitted.
        self._id_counter = 0

        # The executor responsible for making Omics API transfer requests
        self._request_executor = BoundedExecutor(
            max_size=self._config.max_request_queue_size,
            max_num_threads=self._config.max_request_concurrency,
            executor_cls=executor_cls,
        )

        # The executor responsible for submitting the necessary tasks to
        # perform the desired transfer
        self._submission_executor = BoundedExecutor(
            max_size=self._config.max_submission_queue_size,
            max_num_threads=self._config.max_submission_concurrency,
            executor_cls=executor_cls,
        )

        # There is one thread available for writing to disk. It will handle
        # downloads for all files.
        self._io_executor = BoundedExecutor(
            max_size=self._config.max_io_queue_size,
            max_num_threads=1,
            executor_cls=executor_cls,
        )

    @property
    def client(self) -> OmicsClient:
        """Get the client object."""
        return self._client

    @property
    def config(self) -> OmicsTransferConfig:
        """Get the config object."""
        return self._config

    def download_readset(
        self,
        sequence_store_id: str,
        readset_id: str,
        fileobj: Union[IO[Any], str, bytes],
        file: ReadSetFile = ReadSetFile.SOURCE1,
        file_metadata: Dict[str, str] = None,
        subscribers: List[BaseSubscriber] = None,
    ) -> TransferFuture:
        """Download a read set.

        Args:
            sequence_store_id: The sequence store id containing the readset

            readset_id: The readset id which is to be downloaded

            fileobj: The name of a file to download or a seekable file-like
                object to download. It is recommended to use a filename because
                file-like objects may result in higher memory usage.

            file: File to download. By default, ReadSetFile.SOURCE1 is used.

            file_metadata: File metadata containing information about the file

            subscribers: The list of subscribers to be invoked in the
                order provided based on the event emit during the process of
                the transfer request.

        Returns: TransferFuture object for the task.
        """
        if subscribers is None:
            subscribers = []
        if file_metadata is None:
            file_metadata = {}
        validate_all_known_args(file_metadata, REQUIRED_FILE_METADATA_ARGS)
        file = ReadSetFile(file)

        call_args = CallArgs(
            sequence_store_id=sequence_store_id,
            readset_id=readset_id,
            fileobj=fileobj,
            file=file.value,
            file_metadata=file_metadata,
            subscribers=subscribers,
        )
        extra_main_kwargs = {"io_executor": self._io_executor}
        return self._submit_transfer(call_args, ReadSetDownloadSubmissionTask, extra_main_kwargs)

    def download_reference(
        self,
        reference_store_id: str,
        reference_id: str,
        fileobj: Union[IO[Any], str, bytes],
        file: ReferenceFile = ReferenceFile.SOURCE,
        file_metadata: Dict[str, str] = None,
        subscribers: List[BaseSubscriber] = None,
    ) -> TransferFuture:
        """Download a reference file.

        Args:
            reference_store_id: The sequence store id containing the reference.

            reference_id: The reference id which is to be downloaded.

            fileobj: The name of a file to download or a seekable file-like
                object to download. It is recommended to use a filename because
                file-like objects may result in higher memory usage.

            file: File to download. By default, ReferenceFile.SOURCE is used.

            file_metadata: File metadata containing information about the file

            subscribers: The list of subscribers to be invoked in the
                order provided based on the event emit during the process of
                the transfer request.

        Returns: TransferFuture object for the task.
        """
        if subscribers is None:
            subscribers = []
        if file_metadata is None:
            file_metadata = {}
        validate_all_known_args(file_metadata, REQUIRED_FILE_METADATA_ARGS)
        file = ReferenceFile(file)

        call_args = CallArgs(
            reference_store_id=reference_store_id,
            reference_id=reference_id,
            fileobj=fileobj,
            file=file.value,
            file_metadata=file_metadata,
            subscribers=subscribers,
        )
        extra_main_kwargs = {"io_executor": self._io_executor}
        return self._submit_transfer(call_args, ReferenceDownloadSubmissionTask, extra_main_kwargs)

    def _submit_transfer(
        self,
        call_args: CallArgs,
        submission_task_cls: Type[OmicsDownloadSubmissionTask],
        extra_main_kwargs: Dict[str, Any] = None,
    ) -> TransferFuture:
        if not extra_main_kwargs:
            extra_main_kwargs = {}

        # Create a TransferFuture to return back to the user
        transfer_future, components = self._get_future_with_components(call_args)

        # Add any provided done callbacks to the created transfer future
        # to be invoked on the transfer future being complete.
        for callback in get_callbacks(transfer_future, "done"):
            components["coordinator"].add_done_callback(callback)

        # Get the main kwargs needed to instantiate the submission task
        main_kwargs = self._get_submission_task_main_kwargs(transfer_future, extra_main_kwargs)
        # Submit a SubmissionTask that will submit all of the necessary
        # tasks needed to complete the omics transfer.
        self._submission_executor.submit(
            submission_task_cls(
                transfer_coordinator=components["coordinator"],
                main_kwargs=main_kwargs,
            )
        )
        # Increment the unique id counter for future transfer requests
        self._id_counter += 1
        return transfer_future

    def _get_future_with_components(
        self, call_args: CallArgs
    ) -> Tuple[TransferFuture, Dict[str, Any]]:
        transfer_id = self._id_counter
        # Creates a new transfer future along with its components
        transfer_coordinator = TransferCoordinator(transfer_id=str(transfer_id))
        # Track the transfer coordinator for transfers to manage.
        self._coordinator_controller.add_transfer_coordinator(transfer_coordinator)
        # Also make sure that the transfer coordinator is removed once
        # the transfer completes so it does not stick around in memory.
        transfer_coordinator.add_done_callback(
            self._coordinator_controller.remove_transfer_coordinator,
            transfer_coordinator,
        )
        components = {
            "meta": TransferMeta(call_args, transfer_id=str(transfer_id)),
            "coordinator": transfer_coordinator,
        }
        transfer_future = TransferFuture(**components)
        return transfer_future, components

    def _get_submission_task_main_kwargs(
        self, transfer_future: TransferFuture, extra_main_kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        main_kwargs = {
            "client": self._client,
            "config": self._config,
            "osutil": self._osutil,
            "request_executor": self._request_executor,
            "transfer_future": transfer_future,
        }
        main_kwargs.update(extra_main_kwargs)
        return main_kwargs

    def __enter__(self) -> Any:
        """Return self when entering a 'with' block."""
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_value: BaseException, *args: Any) -> None:
        """Clean up when a 'with' block is complete."""
        cancel = False
        cancel_msg = ""
        cancel_exc_type: Type[BaseException] = FatalError
        # If a exception was raised in the context handler, signal to cancel
        # all of the in progress futures in the shutdown.
        if exc_type:
            cancel = True
            cancel_msg = str(exc_value)
            if not cancel_msg:
                cancel_msg = repr(exc_value)
            # If it was a KeyboardInterrupt, the cancellation was initiated
            # by the user.
            if isinstance(exc_value, KeyboardInterrupt):
                cancel_exc_type = CancelledError
        self._shutdown(cancel, cancel_msg, cancel_exc_type)

    def shutdown(self, cancel: bool = False, cancel_msg: str = "") -> None:
        """Shut down the Transfer Manager.

        It will wait till all transfers complete before it completely shuts
        down.

        Args:
            cancel: If True, calls TransferFuture.cancel() for
                all in-progress in transfers. This is useful if you want the
                shutdown to happen quicker.

            cancel_msg: The message to specify if canceling all in-progress
                transfers.
        """
        self._shutdown(cancel, cancel_msg)

    def _shutdown(
        self,
        cancel: bool,
        cancel_msg: str,
        exc_type: Type[BaseException] = CancelledError,
    ) -> None:
        if cancel:
            # Cancel all in-flight transfers if requested, before waiting
            # for them to complete.
            self._coordinator_controller.cancel(cancel_msg, exc_type)
        try:
            # Wait until there are no more in-progress transfers. This is
            # wrapped in a try statement because this can be interrupted
            # with a KeyboardInterrupt that needs to be caught.
            self._coordinator_controller.wait()
        except KeyboardInterrupt:
            # If not errors were raised in the try block, the cancel should
            # have no coordinators it needs to run cancel on. If there was
            # an error raised in the try statement we want to cancel all of
            # the inflight transfers before shutting down to speed that
            # process up.
            self._coordinator_controller.cancel("KeyboardInterrupt()")
            raise
        finally:
            # Shutdown all of the executors.
            self._submission_executor.shutdown()
            self._request_executor.shutdown()
            self._io_executor.shutdown()
