""" Test of basic workflow and job creation features
"""

import spate

import unittest
import itertools
import json
import StringIO

class WorkflowCreationTests (unittest.TestCase):

    def test_workflow_creation (self):
        spate.new_workflow()

    def test_workflow_name_manipulation (self):
        # a workflow name can be provided at creation
        workflow_name_1 = spate.utils.random_string()

        workflow = spate.new_workflow(workflow_name = workflow_name_1)
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
            inputs, outputs = workflow.job_inputs_and_outputs(dummy_job_id)
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

            inputs, outputs = workflow.job_inputs_and_outputs("dummy-id")
            self.assertEqual(inputs, ('a',))
            self.assertEqual(outputs, ('b',))

            template = workflow.job_template("dummy-id")
            self.assertEqual(template, "dummy-template")

            data = workflow.job_data("dummy-id")
            self.assertEqual(data.get("variable_a"), 1)
            self.assertEqual(data.get("variable_b"), 2)

            workflow.remove_job("dummy-id")

    SIMPLE_JSON_EXAMPLE = {
            "workflow": {
                "name": "dummy-workflow"
            },
            "jobs": [
                {
                    "id": "dummy-job-id",
                    "inputs": ["a"],
                    "outputs": ["b"],
                    "template": "dummy-template",
                    "data": {
                        "dummy_variable": 1
                    }
                }
            ]
        }

    def test_job_export_to_json (self):

        def _check_json (obj):
            self.assertTrue(spate.utils.cmp_dict(
                obj, self.SIMPLE_JSON_EXAMPLE))

        # we create a simple workflow and job
        workflow = spate.new_workflow("dummy-workflow")
        workflow.add_job('a', 'b',
            "dummy-template",
            "dummy-job-id",
            dummy_variable = 1)

        # regardless of if we ask outdated jobs only or not,
        for flag in (True, False):
            # the to_json() method should return this JSON document
            _check_json(spate.to_json(workflow, outdated_only = flag))

            # the save() method should also return this JSON document
            io = StringIO.StringIO()
            spate.save(workflow, io, outdated_only = flag)

            _check_json(json.loads(io.getvalue()))

    def test_job_import_from_json (self):

        def _check_workflow (workflow):
            self.assertEqual(workflow.name,
                self.SIMPLE_JSON_EXAMPLE["workflow"]["name"])

            self.assertEqual(workflow.number_of_jobs, 1)
            self.assertEqual(workflow.number_of_paths, 2)
            self.assertTrue("dummy-job-id" in workflow)

            inputs, outputs = workflow.job_inputs_and_outputs("dummy-job-id")
            self.assertEqual(inputs,
                tuple(self.SIMPLE_JSON_EXAMPLE["jobs"][0]["inputs"]))
            self.assertEqual(outputs,
                tuple(self.SIMPLE_JSON_EXAMPLE["jobs"][0]["outputs"]))

            self.assertEqual(
                workflow.job_template("dummy-job-id"),
                self.SIMPLE_JSON_EXAMPLE["jobs"][0]["template"])

            self.assertEqual(
                workflow.job_data("dummy-job-id"),
                self.SIMPLE_JSON_EXAMPLE["jobs"][0]["data"])

        for flag in (True, False):
            # the from_json() method should return the workflow we expect
            _check_workflow(spate.from_json(self.SIMPLE_JSON_EXAMPLE))

            # and so does load()
            io = StringIO.StringIO(json.dumps(self.SIMPLE_JSON_EXAMPLE))
            _check_workflow(spate.load(io))

if (__name__ == "__main__"):
    unittest.main()
