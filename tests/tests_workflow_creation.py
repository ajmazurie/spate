""" Test of basic workflow and job creation features
"""

import spate

import unittest
import itertools

class WorkflowCreationTests (unittest.TestCase):

    def test_workflow_creation (self):
        spate.new_workflow()

    def test_workflow_name_manipulation (self):
        # a workflow name can be provided at creation
        workflow_name_1 = spate.utils.random_string()

        workflow = spate.new_workflow(name = workflow_name_1)
        self.assertEqual(workflow.name, workflow_name_1)

        # a workflow name can be set and retrieved through the 'name' property
        workflow_name_2 = spate.utils.random_string()

        workflow.name = workflow_name_2
        self.assertEqual(workflow.name, workflow_name_2)

        # a workflow name can be set and retrieved through dedicated methods
        workflow_name_3 = spate.utils.random_string()

        workflow.set_name(workflow_name_3)
        self.assertEqual(workflow.get_name(), workflow_name_3)

    def test_jobs_addition_and_deletion (self):
        # a newly created workflow should have no job nor path
        workflow = spate.new_workflow()
        self.assertEqual(workflow.number_of_jobs, 0)
        self.assertEqual(workflow.number_of_paths, 0)

        # add_job() doesn't require a job identifier
        job_id = workflow.add_job('a', 'b')
        workflow.remove_job(job_id)

        # add_job() only accepts strings for job identifier
        for job_id in (True, 123, 1.23, {}, []):
            with self.assertRaises(ValueError):
                workflow.add_job('a', 'b', job_id = job_id)

        # add_job() accepts either a string or a
        # list of strings for inputs and outputs
        input_sets = ('a', ('a', 'b'), ('a', 'b', 'c'))
        output_sets = ('x', ('x', 'y'), ('x', 'y', 'z'))
        path_sets = itertools.product(input_sets, output_sets)

        for n, (input_set, output_set) in enumerate(path_sets):
            # after adding this job...
            dummy_job_id = "dummy-%d" % n
            created_job_id = workflow.add_job(
                input_set, output_set,
                job_id = dummy_job_id)

            # add_job() should return the job id we provided
            self.assertEqual(dummy_job_id, created_job_id)

            # we should not be able to add a job with the same name
            with self.assertRaises(spate.SpateException):
                workflow.add_job('i', 'j', job_id = dummy_job_id)

            # we should see this job
            self.assertTrue(workflow.has_job(dummy_job_id))
            self.assertTrue(dummy_job_id in workflow)

            # we should see the corresponding number of jobs and paths
            self.assertEqual(workflow.number_of_jobs, 1)
            self.assertEqual(workflow.number_of_paths,
                len(input_set) + len(output_set))

            # we should see these paths
            inputs, outputs = workflow.get_job_paths(dummy_job_id)
            self.assertEqual(inputs, tuple(input_set))
            self.assertEqual(outputs, tuple(output_set))

            # after deleting this job...
            workflow.remove_job(dummy_job_id)

            # we should not see this job
            self.assertFalse(workflow.has_job(dummy_job_id))
            self.assertFalse(dummy_job_id in workflow)

            # we should have an empty workflow
            self.assertEqual(workflow.number_of_jobs, 0)
            self.assertEqual(workflow.number_of_paths, 0)

    def test_job_add_keyword_arguments (self):
        # test of the keyword arguments for job_add()
        workflow = spate.new_workflow()

        # first syntax, with explicit keyword arguments
        with_keywords = lambda inputs, outputs, template, job_id, **kwargs: \
            workflow.add_job(
                inputs = inputs, outputs = outputs,
                template = template, job_id = job_id, **kwargs)

        # second syntax, without keyword arguments
        without_keywords = lambda inputs, outputs, template, job_id, **kwargs: \
            workflow.add_job(inputs, outputs, template, job_id, **kwargs)

        for add_job in (with_keywords, without_keywords):
            job_id = add_job('a', 'b',
                "dummy-template", "dummy-id",
                variable_a = 1, variable_b = 2)

            self.assertEqual(job_id, "dummy-id")

            inputs, outputs = workflow.get_job_paths("dummy-id")
            self.assertEqual(inputs, ('a',))
            self.assertEqual(outputs, ('b',))

            template = workflow.get_job_template("dummy-id")
            self.assertEqual(template, "dummy-template")

            data = workflow.get_job_data("dummy-id")
            self.assertEqual(data.get("variable_a"), 1)
            self.assertEqual(data.get("variable_b"), 2)

            workflow.remove_job("dummy-id")

    #def test_workflow_equality (self):


if (__name__ == "__main__"):
    unittest.main()
