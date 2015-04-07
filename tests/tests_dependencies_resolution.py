""" Tests of the job dependency resolution algorithms

    TODO:
    - test list_jobs() with temporary files
    - test list_jobs() with a pre-generated DAG
"""

import spate

import unittest
import os
import networkx

class DependenciesResolutionTests (unittest.TestCase):

    def test_output_collision (self):
        workflow = spate.new_workflow()

        # we should not be able to create two jobs that produces the same path
        with self.assertRaises(spate.SpateException):
            workflow.add_job('a', 'b', job_id = "dummy-1")
            workflow.add_job('c', 'b', job_id = "dummy-2")

        # the offending job should have been removed, but not previous ones
        self.assertTrue("dummy-1" in workflow)
        self.assertFalse("dummy-2" in workflow)

        self.assertEqual(workflow.number_of_jobs, 1)
        self.assertEqual(workflow.number_of_paths, 2)

    def test_input_and_output_collision (self):
        workflow = spate.new_workflow()

        # we should not be able to create a job
        # whose input and output paths overlap
        with self.assertRaises(spate.SpateException):
            workflow.add_job(('a', 'b'), ('b', 'c'))

        # the offending job should have been removed
        self.assertEqual(workflow.number_of_jobs, 0)
        self.assertEqual(workflow.number_of_paths, 0)

    def test_dag_enforcement (self):
        workflow = spate.new_workflow()

        # we should not be able to create cycles in a workflow
        with self.assertRaises(spate.SpateException):
            workflow.add_job('a', 'b', job_id = "dummy-1")
            workflow.add_job('b', 'c', job_id = "dummy-2")
            workflow.add_job('c', 'a', job_id = "dummy-3")

        # the offending job should have been removed
        self.assertTrue("dummy-1" in workflow)
        self.assertTrue("dummy-2" in workflow)
        self.assertFalse("dummy-3" in workflow)

        self.assertEqual(workflow.number_of_jobs, 2)
        self.assertEqual(workflow.number_of_paths, 3)

    def test_simple_job_ordering (self):
        workflow = spate.new_workflow()

        # we create a chain of CHAIN_LENGTH jobs that each
        # use the path produced by the previous job
        CHAIN_LENGTH = 10
        for i in range(CHAIN_LENGTH):
            input_path_name = "dummy_path_%d" % i
            self.assertFalse(os.path.exists(input_path_name))
            output_path_name = "dummy_path_%d" % (i+1)
            self.assertFalse(os.path.exists(output_path_name))

            workflow.add_job(
                input_path_name, output_path_name,
                job_id = "job-%d" % i)

            i += 1

        # regardless of if we are asking for outdated jobs or not,
        for flag in (True, False):
            jobs_list = list(workflow.list_jobs(outdated_only = flag))

            # the list of jobs should have the right length
            self.assertEqual(len(jobs_list), CHAIN_LENGTH)

            # the list of jobs should be in the order they have been declared
            for i, job_id in enumerate(jobs_list):
                self.assertEqual("job-%d" % i, job_id)

if (__name__ == "__main__"):
    unittest.main()
