# Amazon Omics Tools

Tools for working with the Amazon Omics Service.

## Using the Omics Transfer Manager

### Basic Usage
The `TransferManager` class makes it easy to download files for an Omics reference or read set.  By default the files are saved to the current directory, or you can specify a custom location with the `directory` parameter.

```python
import boto3
from omics.common.omics_file_types import ReadSetFileName, ReferenceFileName, ReadSetFileType
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

### Upload specific files
Specific files can be uploaded via the `upload_read_set` method.
The `fileobjs` parameter can be either the name of a local file, or a `TextIO` or `BinaryIO` object that supports read methods.
For paired end reads, you can define `fileobjs` as a list of files.

```python
# Upload a specific read set file.
read_set_id = manager.upload_read_set(
    "my-sequence-data/read-set-file.bam",
    SEQUENCE_STORE_ID,
    ReadSetFileType.BAM,
    "name",
    "subject-id",
    "sample-id",
    "<my-reference-arn>",
)

# Upload paired end read set files.
read_set_id = manager.upload_read_set(
    ["my-sequence-data/read-set-file_1.fastq.gz", "my-sequence-data/read-set-file_2.fastq.gz"],
    SEQUENCE_STORE_ID,
    ReadSetFileType.FASTQ,
    "name",
    "subject-id",
    "sample-id",
    "<my-reference-arn>",
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

## Using the Omics URI Parser
### Basic Usage
The `OmicsUriParser` class makes it easy to parse omics readset and reference URIs to extract fields relevant for calling 
AWS omics APIs.


#### Readset file URI: 
Readset file URIs come in the following format: 
```
omics://<AWS_ACCOUNT_ID>.storage.<AWS_REGION>.amazonaws.com/<SEQUENCE_STORE_ID>/readSet/<READSET_ID>/<SOURCE1/SOURCE2>
```
For example:
```
omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1
omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source2
```

#### Reference file URI:
Reference file URIs come in the following format: 
```
omics://<AWS_ACCOUNT_ID>.storage.<AWS_REGION>.amazonaws.com/<REFERENCE_STORE_ID>/reference/<REFERENCE_ID>/source
```
For example:
```
omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/reference/5346184667/source
```

```python
import boto3
from omics.uriparse.uri_parse import OmicsUriParser, OmicsUri

READSET_URI_STRING = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/readSet/5346184667/source1"
REFERENCE_URI_STRING = "omics://123412341234.storage.us-east-1.amazonaws.com/5432154321/reference/5346184667/source"

client = boto3.client("omics")

readset = OmicsUriParser(READSET_URI_STRING).parse()
reference = OmicsUriParser(REFERENCE_URI_STRING).parse()

# use the parsed fields from the URIs to call omics APIs:

manager = TransferManager(client)

# Download all files for a reference.
manager.download_reference(reference.store_id, reference.resource_id)

# Download all files for a read set to a custom directory.
manager.download_read_set(readset.store_id, readset.resource_id, readset.file_name)

# Download a specific read set file with a custom filename.
manager.download_read_set_file(
    readset.store_id,
    readset.resource_id,
    readset.file_name,
    "my-sequence-data/read-set-index"
)
```

## Security

See [CONTRIBUTING](https://github.com/awslabs/amazon-omics-tools/blob/main/CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
