import io
import os
import tempfile

from s3transfer.futures import TransferFuture
from s3transfer.utils import OSUtils

from omics.common.omics_file_types import (
    ReadSetFileName,
    ReferenceFileName,
)
from omics.transfer.manager import TransferManager, _format_local_filename
from tests.transfer import (
    TEST_CONSTANTS,
    TEST_CONSTANTS_REFERENCE_STORE,
    StubbedClientTest,
)
from tests.transfer.functional import (
    add_abort_upload_response,
    add_complete_upload_response,
    add_create_upload_response,
    add_get_read_set_metadata_response,
    add_get_read_set_responses,
    add_get_reference_metadata_response,
    add_get_reference_responses,
    add_upload_part_response,
)


class TestTransferManager(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self.osutils = OSUtils()
        self.transfer_manager = TransferManager(self.client)

    def test_download_read_set_single_file(self):
        add_get_read_set_metadata_response(self.stubber)
        add_get_read_set_responses(self.stubber)

        self.transfer_manager.download_read_set_file(
            TEST_CONSTANTS["sequence_store_id"],
            TEST_CONSTANTS["read_set_id"],
            ReadSetFileName.SOURCE1,
            "test_file",
        )
        self.stubber.assert_no_pending_responses()

    def test_download_read_set_invalid_file_fails_with_exception(self):
        add_get_read_set_metadata_response(self.stubber)

        with self.assertRaises(AttributeError):
            self.transfer_manager.download_read_set_file(
                TEST_CONSTANTS["sequence_store_id"],
                TEST_CONSTANTS["read_set_id"],
                "test_file_obj",
                "invalid_file",
            )

    def test_download_reference_single_file(self):
        add_get_reference_metadata_response(self.stubber)
        add_get_reference_responses(self.stubber)

        self.transfer_manager.download_reference_file(
            TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
            TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
            ReferenceFileName.SOURCE,
            "test_file",
        )
        self.stubber.assert_no_pending_responses()

    def test_download_reference_invalid_file_fails_with_exception(self):
        with self.assertRaises(AttributeError):
            self.transfer_manager.download_reference_file(
                TEST_CONSTANTS_REFERENCE_STORE["reference_store_id"],
                TEST_CONSTANTS_REFERENCE_STORE["reference_id"],
                "test_file_obj",
                "invalid_file",
            )

    def test_download_with_invalid_directory(self):
        add_get_read_set_metadata_response(self.stubber, files=["source1"])
        add_get_read_set_responses(self.stubber, file="SOURCE1")

        with open(os.path.join(self.tempdir, "not-a-directory.txt"), "w") as f:
            with self.assertRaises(TypeError):
                self.transfer_manager.download_read_set(
                    TEST_CONSTANTS["sequence_store_id"],
                    TEST_CONSTANTS["read_set_id"],
                    f,
                )

    def test_format_local_filename_with_lowercase_file_type(self):
        filename = _format_local_filename("test-filename", ReferenceFileName.INDEX, "fasta")
        self.assertEqual(filename, "test-filename.fasta.fai")

    def test_format_fasta_index_local_filename(self):
        filename = _format_local_filename("test-filename", ReferenceFileName.INDEX, "FASTA")
        self.assertEqual(filename, "test-filename.fasta.fai")

    def test_format_fasta_source_local_filename(self):
        filename = _format_local_filename("test-filename", ReferenceFileName.SOURCE, "FASTA")
        self.assertEqual(filename, "test-filename.fasta")

    def test_format_bam_index_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.INDEX, "BAM")
        self.assertEqual(filename, "test-filename.bam.bai")

    def test_format_bam_source1_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.SOURCE1, "BAM", True)
        self.assertEqual(filename, "test-filename_1.bam")

    def test_format_bam_source2_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.SOURCE2, "BAM", True)
        self.assertEqual(filename, "test-filename_2.bam")

    def test_format_cram_index_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.INDEX, "CRAM")
        self.assertEqual(filename, "test-filename.cram.crai")

    def test_format_cram_source1_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.SOURCE1, "CRAM", True)
        self.assertEqual(filename, "test-filename_1.cram")

    # This shouldn't happen, but we create a file with a `.index` extension anyway
    def test_format_fastq_index_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.INDEX, "FASTQ")
        self.assertEqual(filename, "test-filename.index")

    # UBAM should not have an .index file but we include this for consistency.
    def test_format_ubam_index_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.INDEX, "UBAM")
        self.assertEqual(filename, "test-filename.index")

    def test_format_gz_local_filename(self):
        filename = _format_local_filename("test-filename", ReadSetFileName.SOURCE1, "FASTQ", True)
        self.assertEqual(filename, "test-filename_1.fastq")

    def test_format_complicated_local_filename(self):
        extension = ".bam"
        filename_base = "HG001.GRCh38_full_plus_hs38d1_analysis_set_minus_alts.300x"
        filename = _format_local_filename(
            filename_base + extension, ReadSetFileName.SOURCE1, "BAM", True
        )
        self.assertEqual(filename, filename_base + "_1" + extension)

        filename_base = "TestFilenameWithWeirdChars abc...xzy1234567890_!@నేనుÆды.-test-.ext"
        expected_filename_base = "TestFilenameWithWeirdChars_abc...xzy1234567890_ననÆды.-test-.ext"
        filename = _format_local_filename(
            filename_base + extension, ReadSetFileName.SOURCE1, "BAM", True
        )
        self.assertEqual(filename, expected_filename_base + "_1" + extension)

    def test_format_local_filename_removes_original_extension(self):
        filename = _format_local_filename(
            "test-filename.FASTQ.GZ", ReadSetFileName.SOURCE1, "CRAM", True
        )
        self.assertEqual(filename, "test-filename_1.cram")

    def test_upload_single_file(self):
        add_create_upload_response(self.stubber)
        add_upload_part_response(self.stubber, 1, ReadSetFileName.SOURCE1)
        add_complete_upload_response(self.stubber)

        read_set_id = self.run_simple_upload(io.BytesIO(b"some file content1"))

        self.assertEqual(read_set_id, TEST_CONSTANTS["read_set_id"])
        self.stubber.assert_no_pending_responses()

    def test_upload_multiple_files(self):
        add_create_upload_response(self.stubber)
        add_upload_part_response(self.stubber, 1, ReadSetFileName.SOURCE1)
        add_upload_part_response(self.stubber, 1, ReadSetFileName.SOURCE2)
        add_complete_upload_response(self.stubber)

        read_set_id = self.run_simple_upload([io.BytesIO(b"content1"), io.BytesIO(b"content2")])

        self.assertEqual(read_set_id, TEST_CONSTANTS["read_set_id"])
        self.stubber.assert_no_pending_responses()

    def test_upload_bad_file_throws_exception(self):
        add_create_upload_response(self.stubber)
        add_abort_upload_response(self.stubber)

        with self.assertRaises(RuntimeError):
            self.run_simple_upload(b"some file content1").result()
        self.stubber.assert_no_pending_responses()

    def test_upload_too_many_files_throws_exception(self):
        with self.assertRaises(AttributeError):
            self.run_simple_upload(
                [io.BytesIO(b"content1"), io.BytesIO(b"content2"), io.BytesIO(b"content3")]
            ).result()
        self.stubber.assert_no_pending_responses()

    def test_upload_paired_with_wrong_file_type_throws_exception(self):
        with self.assertRaises(AttributeError):
            self.run_simple_upload(
                [io.BytesIO(b"content1"), io.BytesIO(b"content2")], "BAM"
            ).result()
        self.stubber.assert_no_pending_responses()

    def test_upload_no_reference_with_BAM_file_type_exception(self):
        with self.assertRaises(AttributeError):
            self.self.transfer_manager.upload_read_set(
                io.BytesIO(b"some file content1"),
                TEST_CONSTANTS["sequence_store_id"],
                "BAM",
                "name",
                "subjectId",
                "sampleId",
            ).result()

        self.stubber.assert_no_pending_responses()

    def test_upload_no_reference_with_BAM_file_type_exception(self):
        with self.assertRaises(AttributeError):
            self.self.transfer_manager.upload_read_set(
                io.BytesIO(b"some file content1"),
                TEST_CONSTANTS["sequence_store_id"],
                "CRAM",
                "name",
                "subjectId",
                "sampleId",
            ).result()

        self.stubber.assert_no_pending_responses()
    def run_simple_upload(
        self, files: any, file_type: str = "FASTQ"
    ) -> TransferFuture:
        return self.transfer_manager.upload_read_set(
            files,
            TEST_CONSTANTS["sequence_store_id"],
            file_type,
            "name",
            "subjectId",
            "sampleId",
            "referenceArn",
        )
