import unittest

import omics.cli.run_analyzer.utils as utils


class TestRunAnalyzerUtils(unittest.TestCase):
    def test_engine_names(self):
        self.assertEqual(utils.ENGINES, set(["CWL", "WDL", "NEXTFLOW"]))

    def test_task_base_name(self):
        # CWL
        self.assertEqual(utils.task_base_name("test", "CWL"), "test")
        self.assertEqual(utils.task_base_name("test_1", "CWL"), "test")
        self.assertEqual(utils.task_base_name("test_again_1", "CWL"), "test_again")
        # WDL
        self.assertEqual(utils.task_base_name("test", "WDL"), "test")
        self.assertEqual(utils.task_base_name("test-01-1234", "WDL"), "test")
        self.assertEqual(utils.task_base_name("test_again-10-2345", "WDL"), "test_again")
        # Nextflow
        self.assertEqual(utils.task_base_name("test", "NEXTFLOW"), "test")
        self.assertEqual(utils.task_base_name("TEST:MODULE:FOO", "NEXTFLOW"), "TEST:MODULE:FOO")
        self.assertEqual(
            utils.task_base_name("TEST:MODULE:FOO (input1)", "NEXTFLOW"), "TEST:MODULE:FOO"
        )

    def test_task_base_name_invalid_engine(self):
        self.assertRaises(ValueError, utils.task_base_name, "test", "INVALID")

    def test_omics_instance_weight(self):
        def _weight(instance):
            return utils.omics_instance_weight(instance)

        self.assertTrue(_weight("omics.c.2xlarge") < _weight("omics.c.4xlarge"))
        self.assertTrue(_weight("omics.c.4xlarge") < _weight("omics.m.4xlarge"))
        self.assertTrue(_weight("omics.m.4xlarge") < _weight("omics.r.4xlarge"))
        self.assertTrue(_weight("omics.r.4xlarge") < _weight("omics.g4dn.4xlarge"))
        self.assertTrue(_weight("omics.r.4xlarge") < _weight("omics.g5.4xlarge"))
