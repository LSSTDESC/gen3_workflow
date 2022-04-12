import os
import shutil
import unittest
import subprocess
from desc.gen3_workflow import query_workflow

class QueryWorkflowTestCase(unittest.TestCase):
    """TestCase class for query_workflow function."""
    def setUp(self):
        """
        Set up the test repo with a ParslGraph object, but without
        executing any pipetasks.
        """
        tests_dir = os.path.join(os.environ['GEN3_WORKFLOW_DIR'], 'tests')
        self.tmp_dir = os.path.join(tests_dir, 'tmp_query_workflow')
        if os.path.isdir(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)
        os.makedirs(self.tmp_dir)
        os.chdir(self.tmp_dir)
        for item in ('parsl_graph_init.py', 'bps_cpBias.yaml',
                     'run_cpBias.sh', 'cpBias.yaml'):
            shutil.copy(os.path.join(tests_dir, 'cpBias_test', item), '.')
        command = 'bash ./run_cpBias.sh ./parsl_graph_init.py'
        subprocess.check_call(command, shell=True)

    def tearDown(self):
        if os.path.isdir(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

    def test_query_workflow(self):
        workflow_name = 'u/lsst/bot_13035_R22_S11_cpBias/test_run'
        df = query_workflow(workflow_name)
        self.assertEqual(len(df), 0)

if __name__ == '__main__':
    unittest.main()
