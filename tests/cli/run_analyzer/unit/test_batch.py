import io
import unittest

import omics.cli.run_analyzer.batch as batch

class TestRunAnalyzerBatch(unittest.TestCase):
    def test_do_aggregation(self):
        resources = [{'key': 2}, {'key': 1}, {'key': 4}]
        result = batch._do_aggregation(resources, 'key', "count")
        self.assertEqual(result, 3)
        result = batch._do_aggregation(resources, 'key', "sum")
        self.assertEqual(result, 7)
        result = batch._do_aggregation(resources, 'key', "maximum")
        self.assertEqual(result, 4)
        result = batch._do_aggregation(resources, 'key', "mean")
        self.assertEqual(result, 2.33)
        result = batch._do_aggregation(resources, 'key', 'stdDev')
        self.assertEqual(result, 1.53)
        
    def test_do_aggregation_with_bad_operation(self):
        resources = [{'key': 2}, {'key': 1}, {'key': 4}]
        self.assertRaises(ValueError, batch._do_aggregation, resources, 'key', "bad_operation")
    
    def test_aggregate_resources(self):
        resources=[
            {   
                'type': 'run', 'runningSeconds': 10.0, 'cpuUtilizationRatio': 1.0, 'memoryUtilizationRatio': 1.0,
                 'gpusReserved': 0, 'recommendedCpus': 4, 'recommendedMemoryGiB': 8, 
                 'omicsInstanceTypeMinimum': 'omics.c.large', 'estimatedUSD':1.00,
                 'name': 'foo-01-000'
            },
            {
                'type': 'run', 'runningSeconds': 20.0, 'cpuUtilizationRatio': 1.0, 'memoryUtilizationRatio': 1.0,
                'gpusReserved': 0, 'recommendedCpus': 4, 'recommendedMemoryGiB': 8,
                'omicsInstanceTypeMinimum': 'omics.c.large', 'estimatedUSD':1.00,
                'name': 'foo-02-000'
            }
        ]
        with io.StringIO() as result:
            batch._aggregate_resources(resources=resources, name='foo', engine='WDL', out=result)
            self.assertEqual(result.getvalue(), 'run,foo,2,15.0,20.0,7.07,1.0,1.0,0,4,8,omics.c.large,1.0,1.0\n')


        
