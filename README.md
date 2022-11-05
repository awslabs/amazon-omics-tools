# Amazon Omics Tools

Tools for working with the Amazon Omics Service.

## Using the Omics Transfer Manager

### Basic Usage
```python
import boto3
from omics.transfer import ReferenceFileName, ReadSetFileName
from omics.transfer.manager import TransferManager

REFERENCE_STORE_ID = "<my-reference-store-id>"
SEQUENCE_STORE_ID = "<my-sequence-store-id>"

client = boto3.client("omics", "us-west-2")
manager = TransferManager(client)

# Download all files for a reference.
# By default they will be stored in a directory called "omics-data"
# or you can specify a custom directory.
manager.download_reference(REFERENCE_STORE_ID, "<my-reference-id>")

# Download all files for a read set.
# By default they will be stored in a directory called "omics_read_sets".
manager.download_read_set(SEQUENCE_STORE_ID, "<my-read-set-id>")
```

### Download specific files
```python

# Download a specific reference file.
# By default it will be stored in "omics-data".
manager.download_reference_file(
    REFERENCE_STORE_ID,
    "<my-reference-id>",
    ReferenceFileName.INDEX
)

# Download a specific read set file.
# You can specify a custom filename.
manager.download_read_set_file(
    SEQUENCE_STORE_ID,
    "<my-read-set-id>",
    ReadSetFileName.SOURCE1,
    "my-sequence-data/read-set-source1"
)
```

### Subscribe to events
```python

class ProgressReporter(OmicsTransferSubscriber):
    def on_queued(self, **kwargs):
        future: OmicsTransferFuture = kwargs["future"]
        print(f"Download queued: {future.meta.call_args.fileobj}")

    def on_done(self, **kwargs):
        print("Download complete")

manager.download_read_set(SEQUENCE_STORE_ID, "<my-read-set-id>", subscribers=[ProgressReporter()])
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
