import unittest
from omics.cli.run_analyzer import writeconfig

class TestGetBaseTask(unittest.TestCase):
    def test_get_base_task_nextflow(self):
        result = writeconfig.get_base_task('NEXTFLOW', 'task1 (sample1)')
        self.assertEqual(result, 'task1')

    def test_get_base_task_cwl(self):
        result = writeconfig.get_base_task('CWL', 'task1 (sample1)')
        self.assertRaises(ValueError)

if __name__ == '__main__':
    unittest.main()