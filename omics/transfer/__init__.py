from enum import Enum
from typing import IO, Any, List, Type, Union

from s3transfer.futures import TransferFuture
from s3transfer.subscribers import BaseSubscriber


class OmicsTransferSubscriber(BaseSubscriber):
    """Base class for subscribers of Omics data transfer."""


class OmicsTransferFuture(TransferFuture):
    """Future for getting the result of Omics data transfer."""


class ExtendedEnum(Enum):
    """Enum subclass that includes helper methods."""

    @classmethod
    def list(cls: Type[Enum]) -> List[str]:
        """Return the list of allowed values in an Enum."""
        return list(map(lambda c: c.value, cls))  # type: ignore

    @classmethod
    def from_object(cls: Type[Enum], object: Any) -> Enum:
        """Convert an object to the Enum type if possible.

        Since Python allows strings to be passed as parameters even when an Enum
        type is specified, this method converts a string to an enum if it
        matches one of the allowed values.

        Args:
            object: the object to convert to an Enum.
        """
        if type(object) == cls:
            return object
        if type(object) == str:
            for enum in cls:
                if object.upper() == enum.value:
                    return enum
            valid_values = ", ".join(cls.list())
            raise AttributeError(f"{cls.__name__} must be one of {valid_values}")

        raise AttributeError(f"Unsupported type for {cls.__name__}: {type(object)}")


class ReadSetFileName(ExtendedEnum):
    """Available read set file names."""

    SOURCE1 = "SOURCE1"
    SOURCE2 = "SOURCE2"
    INDEX = "INDEX"


class ReferenceFileName(ExtendedEnum):
    """Available reference file names."""

    SOURCE = "SOURCE"
    INDEX = "INDEX"


class FileTransferDirection(ExtendedEnum):
    """Available transfer directions (UP = upload, DOWN = download)."""

    UP = "UP"
    DOWN = "DOWN"


class OmicsFileType(ExtendedEnum):
    """Available file types."""

    READ_SET = "READ_SET"
    REFERENCE = "REFERENCE"


class FileTransfer:
    """Details of an Omics file transfer."""

    def __init__(
        self,
        store_id: str,
        file_set_id: str,
        filename: str,
        fileobj: Union[IO[Any], str],
        omics_file_type: OmicsFileType,
        direction: FileTransferDirection = FileTransferDirection.DOWN,
        subscribers: List[BaseSubscriber] = None,
    ):
        """Details of a file download.

        Args:
            store_id: the ID of the data store (either Reference Store or Sequence Store).

            file_set_id: Reference ID or Read Set ID.

            filename: the name of the file when it it stored on the server.

            fileobj: The name of a file or IO object to transfer data to.

            omics_file_type: the type of Omics file being transferred.

            orig_filename: The original name of the data file (ex: "NA12878.cram")

            direction: currently only DOWN (download) is supported.

            subscribers: The list of subscribers to be invoked in the
                order provided based on the event emit during the process of
                the transfer request.
        """
        self.direction = direction
        self.omics_file_type = omics_file_type
        self.store_id = store_id
        self.file_set_id = file_set_id
        self.filename = filename
        self.fileobj = fileobj
        self.subscribers = [] if subscribers is None else subscribers

        if filename is None:
            raise AttributeError("filename cannot be None")
        if direction != FileTransferDirection.DOWN:
            raise AttributeError("Only download is currently supported (direction = DOWN)")
