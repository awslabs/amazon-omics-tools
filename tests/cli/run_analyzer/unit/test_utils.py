import unittest

import botocore.session
from botocore.stub import Stubber

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

    def test_get_engine_with_workflow_arn(self):
        session = botocore.session.get_session()
        region = "us-west-2"
        omics = session.create_client(
            "omics",
            region,
            aws_access_key_id="foo",
            aws_secret_access_key="bar",
        )
        stubber = Stubber(omics)
        workflow_arn = "arn:aws:omics:us-east-1:123456789012:workflow/9876"
        stubber.add_response(
            "get_workflow",
            {
                "arn": workflow_arn,
                "id": "9876",
                "status": "ACTIVE",
                "type": "PRIVATE",
                "name": "hello",
                "engine": "WDL",
                "main": "main.wdl",
                "digest": "sha256:367f76a49c1e6f412a6fb319fcc7061d78ad612d06a9b8ef5b5e5f2e17a32e6f",
                "parameterTemplate": {
                    "param": {"description": "desc"},
                },
                "creationTime": "2024-04-19T14:38:56.492330+00:00",
                "statusMessage": "status",
                "tags": {},
            },
            {"id": "9876"},
        )
        stubber.activate()
        self.assertEqual(utils.get_engine(workflow_arn, client=omics), "WDL")
        stubber.deactivate()
