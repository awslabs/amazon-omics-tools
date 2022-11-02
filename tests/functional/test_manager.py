import os
import tempfile
from unittest import mock

from s3transfer.futures import BaseExecutor

from omics_transfer import CancelledError, FatalError
from omics_transfer.manager import OmicsTransferConfig, OmicsTransferManager
from tests import StubbedClientTest
from tests.functional import (
    add_get_readset_metadata_response,
    add_get_readset_responses,
    add_get_reference_metadata_response,
    add_get_reference_responses,
    create_download_readset_call_kwargs,
    create_download_reference_call_kwargs,
)


class ArbitraryException(Exception):
    pass


class OmicsTransferManagerTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, "test_file")
        self._manager = OmicsTransferManager(
            self.client,
            OmicsTransferConfig(max_request_concurrency=1, max_submission_concurrency=1),
        )

    @property
    def manager(self):
        return self._manager

    def test_readset_error_in_context_manager_cancels_incomplete_transfers(self):
        num_transfers = 100
        futures = []
        ref_exception_msg = "arbitrary exception"
        for _ in range(num_transfers):
            add_get_readset_metadata_response(self.stubber)
            add_get_readset_responses(self.stubber)

        try:
            with self.manager:
                for i in range(num_transfers):
                    futures.append(
                        self.manager.download_readset(
                            **create_download_readset_call_kwargs(self.filename)
                        )
                    )
                raise ArbitraryException(ref_exception_msg)
        except ArbitraryException:
            # At least one of the submitted futures should have been
            # cancelled.
            with self.assertRaisesRegex(FatalError, ref_exception_msg):
                for future in futures:
                    future.result()

    def test_readset_control_c_in_context_manager_cancels_incomplete_transfers(self):
        num_transfers = 100
        futures = []

        for _ in range(num_transfers):
            add_get_readset_metadata_response(self.stubber)
            add_get_readset_responses(self.stubber)

        try:
            with self.manager:
                for i in range(num_transfers):
                    futures.append(
                        self.manager.download_readset(
                            **create_download_readset_call_kwargs(self.filename)
                        )
                    )
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            with self.assertRaisesRegex(CancelledError, "KeyboardInterrupt()"):
                for future in futures:
                    future.result()

    def test_reference_error_in_context_manager_cancels_incomplete_transfers(self):
        num_transfers = 100
        futures = []
        ref_exception_msg = "arbitrary exception"
        for _ in range(num_transfers):
            add_get_reference_metadata_response(self.stubber)
            add_get_reference_responses(self.stubber)

        try:
            with self.manager:
                for i in range(num_transfers):
                    futures.append(
                        self.manager.download_reference(
                            **create_download_reference_call_kwargs(self.filename)
                        )
                    )
                raise ArbitraryException(ref_exception_msg)
        except ArbitraryException:
            # At least one of the submitted futures should have been
            # cancelled.
            with self.assertRaisesRegex(FatalError, ref_exception_msg):
                for future in futures:
                    future.result()

    def test_reference_control_c_in_context_manager_cancels_incomplete_transfers(self):
        num_transfers = 100
        futures = []

        for _ in range(num_transfers):
            add_get_reference_metadata_response(self.stubber)
            add_get_reference_responses(self.stubber)

        try:
            with self.manager:
                for i in range(num_transfers):
                    futures.append(
                        self.manager.download_reference(
                            **create_download_reference_call_kwargs(self.filename)
                        )
                    )
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            with self.assertRaisesRegex(CancelledError, "KeyboardInterrupt()"):
                for future in futures:
                    future.result()

    def test_use_custom_executor_implementation(self):
        mocked_executor_cls = mock.Mock(BaseExecutor)
        manager = OmicsTransferManager(self.client, executor_cls=mocked_executor_cls)
        manager.download_readset(**create_download_readset_call_kwargs(self.filename))
        self.assertTrue(mocked_executor_cls.return_value.submit.called)

    def test_client_property(self):
        manager = OmicsTransferManager(self.client)
        self.assertIs(manager.client, self.client)

    def test_config_property(self):
        config = OmicsTransferConfig()
        manager = OmicsTransferManager(self.client, config)
        self.assertIs(manager.config, config)
