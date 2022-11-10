import os
from concurrent.futures import CancelledError
from typing import IO, Any, List, Type, Union

from mypy_boto3_omics.client import OmicsClient
from s3transfer.download import (
    DownloadFilenameOutputManager,
    DownloadNonSeekableOutputManager,
    DownloadOutputManager,
    DownloadSeekableOutputManager,
)
from s3transfer.exceptions import FatalError
from s3transfer.futures import (
    BoundedExecutor,
    NonThreadedExecutor,
    TransferCoordinator,
    TransferMeta,
)
from s3transfer.manager import TransferCoordinatorController
from s3transfer.utils import OSUtils, get_callbacks

from omics.transfer import (
    FileTransfer,
    FileTransferDirection,
    OmicsFileType,
    OmicsTransferFuture,
    OmicsTransferSubscriber,
    ReadSetFileName,
    ReferenceFileName,
)
from omics.transfer.config import TransferConfig
from omics.transfer.download import DownloadSubmissionTask

DONE_CALLBACK_TYPE = "done"


class TransferManager:
    """Omics data transfer manager which uses multiple threads for parallel processing."""

    def __init__(
        self,
        client: OmicsClient,
        config: TransferConfig = None,
    ):
        """Initialize a Transfer Manager.

        Args:
            client: Client of the Amazon Omics Service.

            config: Configuration details for the Transfer Manager.
        """
        self._client = client
        self._config = config if config is not None else TransferConfig()
        self._osutil = OSUtils()
        self._coordinator_controller = TransferCoordinatorController()
        self._current_transfer_id = 1

        # A counter to create unique id's for each transfer submitted.
        self._id_counter = 0

        # Passing a NonThreadedExecutor to BoundedExecutor causes it to run
        # everything in a single thread.  The default is ThreadPoolExecutor.
        executor_cls = None if self._config.use_threads else NonThreadedExecutor

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

    def download_reference(
        self,
        reference_store_id: str,
        reference_id: str,
        directory: str = None,
        subscribers: List[OmicsTransferSubscriber] = [],
        wait: bool = True,
    ) -> List[OmicsTransferFuture]:
        """Download all files for an Omics reference dataset.

        Args:
            reference_store_id: ID of the Omics reference store.

            reference_id: ID of the Omics reference.

            directory: Local directory to place the files.
                Defaults to the value in TransferConfig.directory.

            subscribers: one or more subscribers for receiving transfer events.

            wait: True = block until all files have been downloaded (default).
                False = return a list of futures for controlling how to wait.
        """
        reference_metadata = self._client.get_reference_metadata(
            referenceStoreId=reference_store_id, id=reference_id
        )

        transfer_futures: List[OmicsTransferFuture] = []
        if directory is None:
            directory = self._config.directory
        _create_directory(directory)

        for filename in reference_metadata["files"]:
            reference_file = ReferenceFileName.from_object(filename.upper())

            file_path = os.path.join(
                directory, f"{reference_store_id}_{reference_id}_{filename.lower()}"
            )

            transfer_future = self.download_reference_file(
                reference_store_id=reference_store_id,
                reference_id=reference_id,
                server_filename=reference_file,
                client_fileobj=file_path,
                subscribers=subscribers,
                wait=False,
            )
            transfer_futures.append(transfer_future)

        if wait:
            for future in transfer_futures:
                future.result()
        return transfer_futures

    def download_reference_file(
        self,
        reference_store_id: str,
        reference_id: str,
        server_filename: ReferenceFileName,
        client_fileobj: Union[IO[Any], str] = None,
        subscribers: List[OmicsTransferSubscriber] = [],
        wait: bool = True,
    ) -> OmicsTransferFuture:
        """Download a single Omics reference file.

        Args:
            reference_store_id: ID of the Omics reference store.

            reference_id: ID of the Omics reference.

            server_filename: name of the file to download from the service.

            client_fileobj: either the name of a file to write to (a string variable)
                or an IO writer (ex: open file or data stream).

            subscribers: one or more subscribers for receiving transfer events.

            wait: True = block until all files have been downloaded (default).
                False = return a list of futures for controlling how to wait.
        """
        server_filename_enum = ReferenceFileName.from_object(server_filename)

        return self._download_file(
            OmicsFileType.REFERENCE,
            reference_store_id,
            reference_id,
            server_filename_enum.value,
            client_fileobj,
            subscribers,
            wait,
        )

    def download_read_set(
        self,
        sequence_store_id: str,
        read_set_id: str,
        directory: str = None,
        subscribers: List[OmicsTransferSubscriber] = [],
        wait: bool = True,
    ) -> List[OmicsTransferFuture]:
        """Download all files for an Omics read set.

        Args:
            sequence_store_id: ID of the Omics sequence store.

            read_set_id: ID of the Omics read set.

            directory: local directory to place the files.
                Defaults to the value in TransferConfig.directory.

            subscribers: one or more subscribers for receiving transfer events.

            wait: True = block until all files have been downloaded (default).
                False = return a list of futures for controlling how to wait.
        """
        read_set_metadata = self._client.get_read_set_metadata(
            sequenceStoreId=sequence_store_id, id=read_set_id
        )

        transfer_futures: List[OmicsTransferFuture] = []
        if directory is None:
            directory = self._config.directory
        _create_directory(directory)

        for filename in read_set_metadata["files"]:
            read_set_file = ReadSetFileName.from_object(filename.upper())

            file_path = os.path.join(
                directory, f"{sequence_store_id}_{read_set_id}_{filename.lower()}"
            )

            transfer_future = self.download_read_set_file(
                sequence_store_id=sequence_store_id,
                read_set_id=read_set_id,
                server_filename=read_set_file,
                client_fileobj=file_path,
                subscribers=subscribers,
                wait=False,
            )
            transfer_futures.append(transfer_future)

        if wait:
            for future in transfer_futures:
                future.result()
        return transfer_futures

    def download_read_set_file(
        self,
        sequence_store_id: str,
        read_set_id: str,
        server_filename: ReadSetFileName,
        client_fileobj: Union[IO[Any], str] = None,
        subscribers: List[OmicsTransferSubscriber] = [],
        wait: bool = True,
    ) -> OmicsTransferFuture:
        """Download a single Omics read set file.

        Args:
            sequence_store_id: ID of the Omics sequence store.

            read_set_id: ID of the Omics read set.

            server_filename: name of the file to download from the service.

            client_fileobj: either the name of a file to write to (a string variable)
                or an IO writer (ex: open file or data stream).

            subscribers: one or more subscribers for receiving transfer events.

            wait: True = block until all files have been downloaded (default).
                False = return a list of futures for controlling how to wait.
        """
        server_filename_enum = ReadSetFileName.from_object(server_filename)

        return self._download_file(
            OmicsFileType.READ_SET,
            sequence_store_id,
            read_set_id,
            server_filename_enum.value,
            client_fileobj,
            subscribers,
            wait,
        )

    def _download_file(
        self,
        omics_file_type: OmicsFileType,
        store_id: str,
        file_set_id: str,
        server_filename: str,
        client_fileobj: Union[IO[Any], str] = None,
        subscribers: List[OmicsTransferSubscriber] = [],
        wait: bool = False,
    ) -> OmicsTransferFuture:
        """Private helper method for downloading a file."""
        transfer_id = self._get_next_transfer_id()
        transfer_coordinator = TransferCoordinator(transfer_id=transfer_id)

        # Also make sure that the transfer coordinator is removed once
        # the transfer completes so it does not stick around in memory.
        transfer_coordinator.add_done_callback(
            self._coordinator_controller.remove_transfer_coordinator,
            transfer_coordinator,
        )

        if client_fileobj is None:
            # Create a default client file if none is supplied
            _create_directory(self._config.directory)
            client_fileobj = os.path.join(
                self._config.directory,
                f"{store_id}_{file_set_id}_{server_filename.lower()}",
            )

        if DownloadFilenameOutputManager.is_compatible(client_fileobj, self._osutil):
            download_manager: DownloadOutputManager = DownloadFilenameOutputManager(
                self._osutil, transfer_coordinator, self._io_executor
            )
        elif DownloadSeekableOutputManager.is_compatible(client_fileobj, self._osutil):
            download_manager = DownloadSeekableOutputManager(
                self._osutil, transfer_coordinator, self._io_executor
            )
        elif DownloadNonSeekableOutputManager.is_compatible(client_fileobj, self._osutil):
            download_manager = DownloadNonSeekableOutputManager(
                self._osutil, transfer_coordinator, self._io_executor
            )
        else:
            raise ValueError(f"The client_fileobj (type: {type(client_fileobj)}) is not supported")

        file_transfer = FileTransfer(
            store_id=store_id,
            file_set_id=file_set_id,
            filename=server_filename,
            fileobj=client_fileobj,
            subscribers=subscribers,
            direction=FileTransferDirection.DOWN,
            omics_file_type=omics_file_type,
        )

        transfer_meta = TransferMeta(file_transfer, transfer_id=transfer_id)
        transfer_future = OmicsTransferFuture(transfer_meta, transfer_coordinator)

        # Add any provided done callbacks to the created transfer future
        # to be invoked on the transfer future being complete.
        for callback in get_callbacks(transfer_future, DONE_CALLBACK_TYPE):
            transfer_coordinator.add_done_callback(callback)

        # Track the transfer coordinator for transfers to manage.
        self._coordinator_controller.add_transfer_coordinator(transfer_coordinator)

        main_kwargs = {
            "client": self._client,
            "config": self._config,
            "request_executor": self._request_executor,
            "transfer_future": transfer_future,
            "download_manager": download_manager,
            "io_executor": self._io_executor,
        }

        # Submit a SubmissionTask that will submit all of the necessary
        # tasks needed to complete the omics transfer.
        self._submission_executor.submit(
            DownloadSubmissionTask(
                transfer_coordinator=transfer_coordinator,
                main_kwargs=main_kwargs,
            )
        )
        if wait:
            transfer_future.result()

        return transfer_future

    def _get_next_transfer_id(self) -> int:
        self._current_transfer_id += 1
        return self._current_transfer_id - 1

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

        It will wait till all transfers complete before it completely shuts down.

        Args:
            cancel: If True, calls TransferFuture.cancel() for
                all in-progress in transfers. This is useful if you want the
                shutdown to happen faster.

            cancel_msg: The message to specify if canceling all in-progress transfers.
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


def _create_directory(directory: str) -> None:
    """Create a directory if one does not exist yet."""
    if not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)