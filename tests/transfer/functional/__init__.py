import copy
import datetime
import gzip
import io
from io import BytesIO

from botocore.stub import ANY

from omics.common.omics_file_types import ReadSetFileName
from tests.transfer import TEST_CONSTANTS, TEST_CONSTANTS_REFERENCE_STORE


def add_get_read_set_metadata_response(stubber, files=None):
    if files is None:
        files = ["source1"]
    file_metadata = {}
    for file in files:
        file_metadata[file] = {
            "contentLength": len(TEST_CONSTANTS["content"]),
            "partSize": TEST_CONSTANTS["part_size"],
            "totalParts": TEST_CONSTANTS["total_parts"],
        }
    stubber.add_response(
        "get_read_set_metadata",
        {
            "arn": "test_arn",
            "creationTime": "2022-06-21T16:30:32Z",
            "id": TEST_CONSTANTS["read_set_id"],
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "status": "ACTIVE",
            "fileType": "FASTQ",
            "name": "test-read-set",
            "files": file_metadata,
        },
    )


def add_get_read_set_responses(stubber, file="SOURCE1"):
    for i in range(0, len(TEST_CONSTANTS["content"]), TEST_CONSTANTS["part_size"]):
        if i + TEST_CONSTANTS["part_size"] > len(TEST_CONSTANTS["content"]):
            stream = BytesIO(TEST_CONSTANTS["content"][i:])
        else:
            stream = BytesIO(TEST_CONSTANTS["content"][i : i + TEST_CONSTANTS["part_size"]])
        stubber.add_response(
            "get_read_set",
            service_response=copy.deepcopy({"payload": stream}),
            expected_params={
                "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
                "id": TEST_CONSTANTS["read_set_id"],
                "file": file,
                "partNumber": ANY,
            },
        )


def add_gzipped_get_read_set_metadata_response(stubber, files=None):
    # Just use one part when testing gzipped data
    if files is None:
        files = ["source1"]
    file_metadata = {}
    for file in files:
        file_metadata[file] = {
            "contentLength": len(TEST_CONSTANTS["content"]),
            "partSize": len(TEST_CONSTANTS["content"]),
            "totalParts": 1,
        }
    stubber.add_response(
        "get_read_set_metadata",
        {
            "arn": "test_arn",
            "creationTime": "2022-06-21T16:30:32Z",
            "id": TEST_CONSTANTS["read_set_id"],
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "status": "ACTIVE",
            "fileType": "FASTQ",
            "name": "test-read-set",
            "files": file_metadata,
        },
    )


def add_gzipped_get_read_set_response(stubber, file="SOURCE1"):
    stream = io.BytesIO()
    with gzip.open(stream, "wb") as f:
        f.write(TEST_CONSTANTS["content"])
    stream.seek(0)
    stubber.add_response(
        "get_read_set",
        service_response=copy.deepcopy({"payload": stream}),
        expected_params={
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "id": TEST_CONSTANTS["read_set_id"],
            "file": file,
            "partNumber": ANY,
        },
    )


def add_get_reference_metadata_response(stubber, files=None):
    if files is None:
        files = ["source"]
    file_metadata = {}
    for file in files:
        file_metadata[file] = {
            "contentLength": len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
            "partSize": TEST_CONSTANTS_REFERENCE_STORE["part_size"],
            "totalParts": TEST_CONSTANTS_REFERENCE_STORE["total_parts"],
        }
    stubber.add_response(
        "get_reference_metadata",
        {
            "arn": "test_arn",
            "creationTime": "2022-06-21T16:30:32Z",
            "updateTime": "2022-06-22T18:30:32Z",
            "id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            "referenceStoreId": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            "status": "ACTIVE",
            "files": file_metadata,
            "name": "test-reference-file",
            "md5": "eb247690435415724f20d8702e011966",
        },
    )


def add_get_reference_responses(stubber, file="SOURCE"):
    for i in range(
        0,
        len(TEST_CONSTANTS_REFERENCE_STORE["content"]),
        TEST_CONSTANTS_REFERENCE_STORE["part_size"],
    ):
        if i + TEST_CONSTANTS_REFERENCE_STORE["part_size"] > len(
            TEST_CONSTANTS_REFERENCE_STORE["content"]
        ):
            stream = BytesIO(TEST_CONSTANTS_REFERENCE_STORE["content"][i:])
        else:
            stream = BytesIO(
                TEST_CONSTANTS_REFERENCE_STORE["content"][
                    i : i + TEST_CONSTANTS_REFERENCE_STORE["part_size"]
                ]
            )
        stubber.add_response(
            "get_reference",
            service_response=copy.deepcopy({"payload": stream}),
            expected_params={
                "referenceStoreId": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                "id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                "file": file,
                "partNumber": ANY,
            },
        )


def create_download_read_set_call_kwargs(filename):
    return {
        "sequence_store_id": TEST_CONSTANTS["sequence_store_id"],
        "read_set_id": TEST_CONSTANTS["read_set_id"],
        "fileobj": filename,
    }


def create_download_reference_call_kwargs(filename):
    return {
        "reference_store_id": TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
        "reference_id": TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
        "fileobj": filename,
    }


def add_create_upload_response(stubber):
    stubber.add_response(
        "create_multipart_read_set_upload",
        service_response={
            "uploadId": TEST_CONSTANTS["upload_id"],
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "sourceFileType": "ANY",
            "subjectId": "ANY",
            "sampleId": "ANY",
            "referenceArn": "ANY",
            "name": "ANY",
            "creationTime": datetime.datetime.now(),
        },
        expected_params={
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "sourceFileType": ANY,
            "subjectId": ANY,
            "sampleId": ANY,
            "referenceArn": ANY,
            "name": ANY,
        },
    )


def add_upload_part_response(stubber, part_number: int, part_source: ReadSetFileName):
    stubber.add_response(
        "upload_read_set_part",
        service_response={"checksum": TEST_CONSTANTS["checksum"]},
        expected_params={
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "uploadId": TEST_CONSTANTS["upload_id"],
            "partNumber": part_number,
            "partSource": part_source.value,
            "payload": ANY,
        },
    )


def add_abort_upload_response(stubber):
    stubber.add_response(
        "abort_multipart_read_set_upload",
        service_response={},
        expected_params={
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "uploadId": TEST_CONSTANTS["upload_id"],
        },
    )


def add_complete_upload_response(stubber):
    stubber.add_response(
        "complete_multipart_read_set_upload",
        service_response={"readSetId": TEST_CONSTANTS["read_set_id"]},
        expected_params={
            "sequenceStoreId": TEST_CONSTANTS["sequence_store_id"],
            "uploadId": TEST_CONSTANTS["upload_id"],
            "parts": ANY,
        },
    )
