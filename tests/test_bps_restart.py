import os
import shutil
import unittest
import subprocess
import lsst.daf.butler as daf_butler
from desc.gen3_workflow import query_workflow

class BpsRestartTestCase(unittest.TestCase):
    """TestCase class for the `bps restart` command line."""
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
        """Remove the temporary test directory."""
        if os.path.isdir(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

    def test_bps_restart(self):
        """Test the bps restart command for the Parsl plugin."""
        # Run bps restart --id <workflow_name>.
        os.chdir(self.tmp_dir)
        os.environ['BPS_WMS_SERVICE_CLASS'] = 'desc.gen3_workflow.ParslService'
        workflow_name = 'u/lsst/bot_13035_R22_S11_cpBias/test_run'
        command = f'bps restart --id {workflow_name}'
        subprocess.check_call(command, shell=True)
        # Create a butler and check that the expected superbias was
        # created.
        butler = daf_butler.Butler(os.path.join(self.tmp_dir, 'test_repo'),
                                   collections=[workflow_name])
        dsrefs = list(set(butler.registry.queryDatasets('bias',
                                                        instrument='LSSTCam')))
        self.assertEqual(len(dsrefs), 1)
        self.assertEqual(dsrefs[0].dataId['detector'], 94)


if __name__ == '__main__':
    unittest.main()
