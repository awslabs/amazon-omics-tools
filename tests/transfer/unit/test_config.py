import unittest

from omics.transfer.config import TransferConfig


class TestTransferConfig(unittest.TestCase):
    def test_exception_on_zero_attr_value(self):
        with self.assertRaises(ValueError):
            TransferConfig(max_request_queue_size=0)

    def test_exception_on_negative_attr_value(self):
        with self.assertRaises(ValueError):
            TransferConfig(max_request_concurrency=-10)
