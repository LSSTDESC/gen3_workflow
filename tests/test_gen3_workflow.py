"""
Example unit tests for gen3_workflow package
"""
import unittest
import desc.gen3_workflow

class gen3_workflowTestCase(unittest.TestCase):
    def setUp(self):
        self.message = 'Hello, world'

    def tearDown(self):
        pass

    def test_run(self):
        foo = desc.gen3_workflow.gen3_workflow(self.message)
        self.assertEqual(foo.run(), self.message)

    def test_failure(self):
        self.assertRaises(TypeError, desc.gen3_workflow.gen3_workflow)
        foo = desc.gen3_workflow.gen3_workflow(self.message)
        self.assertRaises(RuntimeError, foo.run, True)

if __name__ == '__main__':
    unittest.main()
