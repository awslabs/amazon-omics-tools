# Amazon Omics Tools

Tools for working with the Amazon Omics service.

## Using OmicsTransfer

```python
#!/usr/bin/env python3

from botocore.session import get_session
from omics_transfer import OmicsTransfer

def create_client():
    session = get_session()
    client = session.create_client(
        "omics",
        region_name="us-west-2",
    )
    return client

def test_download_readset():
    # This will download the readset and save it as the filename provided
    # Download time depends on the bandwidth available and network latency
    omics_transfer = OmicsTransfer(create_client())
    omics_transfer.download_readset("<sequence_store_id>", "<read_set_id>", "<file_name>")

def test_download_all():
    # This will download all the readset files and save it under ./download_all_directory/
    omics_transfer = OmicsTransfer(create_client())
    omics_transfer.download_readset_all("<sequence_store_id>", "<read_set_id>", "./download_all_directory/")

test_download_readset()
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
