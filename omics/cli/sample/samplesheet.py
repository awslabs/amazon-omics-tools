import boto3

session = boto3.Session()
omics = session.client("omics")

response = omics.list_read_sets(
    maxResults=100,
    sequenceStoreId="3543742895",
    filter={
        "status": "ACTIVE"
    }
)


for read in response['readSets']:
    id = read['id']
    _ = omics.get_read_set_metadata(id=id, sequenceStoreId="3543742895")
    print(_['files']['source1']['s3Access']['s3Uri'])