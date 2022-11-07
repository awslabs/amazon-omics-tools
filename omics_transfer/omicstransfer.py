# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from typing import IO, Any, List, Type, Union

from mypy_boto3_omics.client import OmicsClient
from s3transfer.futures import BaseExecutor
from s3transfer.subscribers import BaseSubscriber

from omics_transfer.manager import OmicsTransferConfig, OmicsTransferManager
from omics_transfer.utils import (
    ReadSetFile,
    ReferenceFile,
    validate_and_create_directory_if_needed,
)


class OmicsTransfer:
    """A tool for transferring large Omics files using multiple threads to improve performance."""

    def __init__(
        self,
        client: OmicsClient,
        config: OmicsTransferConfig = None,
        executor_cls: Type[BaseExecutor] = None,
    ):
        """Initialize an OmicsTransfer object.

        Args:
            client: Client to be used by the manager

            config: OmicsTransferConfig to associate specific configurations

            executor_cls: The class of executor to use with the transfer
                manager. By default, concurrent.futures.ThreadPoolExecutor is used.
        """
        self._client = client
        if config is None:
            config = OmicsTransferConfig()
        self._config = config
        self._executor_cls = executor_cls
        self._manager = OmicsTransferManager(
            self._client, config=self._config, executor_cls=self._executor_cls
        )

    def download_readset(
        self,
        sequence_store_id: str,
        readset_id: str,
        fileobj: Union[IO[Any], str, bytes],
        file: ReadSetFile = ReadSetFile.SOURCE1,
        subscribers: List[BaseSubscriber] = None,
    ) -> None:
        """Download a read set.

        Args:
            sequence_store_id: The sequence store id containing the readset

            readset_id: The reaset id which is to be downloaded

            fileobj: The name of a file to download or a seekable file-like
                object to download. It is recommended to use a filename because
                file-like objects may result in higher memory usage.

            file: File to download. By default, ReadSetFile.SOURCE1 is used.

            subscribers: The list of subscribers to be invoked in the
                order provided based on the event emit during the process of
                the transfer request.
        """
        future = self._manager.download_readset(
            sequence_store_id, readset_id, fileobj, file=file, subscribers=subscribers
        )
        future.result()

    def download_readset_all(
        self, sequence_store_id: str, readset_id: str, destination_directory: str
    ) -> None:
        """Download all files for a read set.

        Args:
            sequence_store_id: The sequence store id containing the readset

            readset_id: The reaset id which is to be downloaded

            destination_directory: A directory path in which the files will be downloaded
                This method tries to create the directory if it doesn't exist.
        """
        future_list = []
        metadata_response = self._client.get_read_set_metadata(
            sequenceStoreId=sequence_store_id, id=readset_id
        )
        available_files = metadata_response["files"].keys()
        validate_and_create_directory_if_needed(destination_directory)
        for file in available_files:
            file_metadata = {
                "part_size": metadata_response["files"][file]["partSize"],
                "total_parts": metadata_response["files"][file]["totalParts"],
                "content_length": metadata_response["files"][file]["contentLength"],
            }

            fileobj = os.path.join(
                destination_directory, "_".join([file, sequence_store_id, readset_id])
            )
            future = self._manager.download_readset(
                sequence_store_id,
                readset_id,
                fileobj,
                file=ReadSetFile(file.upper()),
                file_metadata=file_metadata,
            )
            future_list.append(future)

        for future in future_list:
            future.result()

    def download_reference(
        self,
        reference_store_id: str,
        reference_id: str,
        fileobj: Union[IO[Any], str, bytes],
        file: ReferenceFile = ReferenceFile.SOURCE,
        subscribers: List[BaseSubscriber] = None,
    ) -> None:
        """Download a reference file.

        Args:
            reference_store_id: The reference store id containing the reference

            reference_id: The reference id which is to be downloaded

            fileobj: The name of a file to download or a seekable file-like
                object to download. It is recommended to use a filename because
                file-like objects may result in higher memory usage.

            file: File to download. By default, ReferenceFile.SOURCE is used.

            subscribers: The list of subscribers to be invoked in the
                order provided based on the event emit during the process of
                the transfer request.
        """
        future = self._manager.download_reference(
            reference_store_id, reference_id, fileobj, file=file, subscribers=subscribers
        )
        future.result()

    def download_reference_all(
        self, reference_store_id: str, reference_id: str, destination_directory: str
    ) -> None:
        """Download all files for a reference.

        Args:
            reference_store_id: The reference store id containing the reference

            reference_id: The reference id which is to be downloaded

            destination_directory: A directory path in which the files will be downloaded
                This method tries to create the directory if it doesn't exist.
        """
        future_list = []
        metadata_response = self._client.get_reference_metadata(
            referenceStoreId=reference_store_id, id=reference_id
        )
        available_files = metadata_response["files"].keys()
        validate_and_create_directory_if_needed(destination_directory)
        for file in available_files:
            file_metadata = {
                "part_size": metadata_response["files"][file]["partSize"],
                "total_parts": metadata_response["files"][file]["totalParts"],
                "content_length": metadata_response["files"][file]["contentLength"],
            }

            fileobj = os.path.join(
                destination_directory, "_".join([file, reference_store_id, reference_id])
            )
            future = self._manager.download_reference(
                reference_store_id,
                reference_id,
                fileobj,
                file=ReferenceFile(file.upper()),
                file_metadata=file_metadata,
            )
            future_list.append(future)

        for future in future_list:
            future.result()
