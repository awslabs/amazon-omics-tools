# Amazon Omics Tools

Tools for working with the Amazon Omics Service.

## Using the Omics Transfer Manager

### Basic Usage
The `TransferManager` class makes it easy to download files for an Omics reference or read set.  By default the files are saved to the current directory, or you can specify a custom location with the `directory` parameter.

```python
import boto3
from omics.transfer import ReferenceFileName, ReadSetFileName
from omics.transfer.manager import TransferManager
from omics.transfer.config import TransferConfig

REFERENCE_STORE_ID = "<my-reference-store-id>"
SEQUENCE_STORE_ID = "<my-sequence-store-id>"

client = boto3.client("omics")
manager = TransferManager(client)

# Download all files for a reference.
manager.download_reference(REFERENCE_STORE_ID, "<my-reference-id>")

# Download all files for a read set to a custom directory.
manager.download_read_set(SEQUENCE_STORE_ID, "<my-read-set-id>", "my-sequence-data")
```

### Download specific files
Specific files can be downloaded via the `download_reference_file` and `download_read_set_file` methods.
The `client_fileobj` parameter can be either the name of a local file to create for storing the data, or a `TextIO` or `BinaryIO` object that supports write methods.

```python
# Download a specific reference file.
manager.download_reference_file(
    REFERENCE_STORE_ID,
    "<my-reference-id>",
    ReferenceFileName.INDEX
)

# Download a specific read set file with a custom filename.
manager.download_read_set_file(
    SEQUENCE_STORE_ID,
    "<my-read-set-id>",
    ReadSetFileName.INDEX,
    "my-sequence-data/read-set-index"
)
```

### Subscribe to events
Transfer events: `on_queued`, `on_progress`, and `on_done` can be observed by defining a subclass of `OmicsTransferSubscriber` and passing in an object which can receive events.

```python
class ProgressReporter(OmicsTransferSubscriber):
    def on_queued(self, **kwargs):
        future: OmicsTransferFuture = kwargs["future"]
        print(f"Download queued: {future.meta.call_args.fileobj}")

    def on_done(self, **kwargs):
        print("Download complete")

manager.download_read_set(SEQUENCE_STORE_ID, "<my-read-set-id>", subscribers=[ProgressReporter()])
```

### Threads
Transfer operations use threads to implement concurrency. Thread use can be disabled by setting the `use_threads` attribute to False.

If thread use is disabled, transfer concurrency does not occur. Accordingly, the value of the `max_request_concurrency` attribute is ignored.

```python
# Disable thread use/transfer concurrency
config = TransferConfig(use_threads=False)
manager = TransferManager(client, config)
manager.download_read_set(SEQUENCE_STORE_ID, "<my-read-set-id>")
```

## Security

See [CONTRIBUTING](https://github.com/awslabs/amazon-omics-tools/blob/main/CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
