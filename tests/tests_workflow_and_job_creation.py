""" Test of basic workflow and job creation features

    TODO: Add a test for workflow union
    TODO: Add a test for workflow comparison
"""

import spate

import unittest
import itertools

class WorkflowCreationTests (unittest.TestCase):

    def test_workflow_creation_with_default_parameters (self):
        spate.new_workflow()

    def test_workflow_creation_with_explicit_parameters (self):
        # a workflow name can be provided at creation
        workflow_name_1 = spate.utils.random_string()
        workflow = spate.new_workflow(name = workflow_name_1)
        self.assertEqual(workflow.name, workflow_name_1)

    def test_workflow_properties_manipulation (self):
        workflow = spate.new_workflow()

        # a workflow name can be set and retrieved through the 'name' property
        workflow_name_1 = spate.utils.random_string()
        workflow.name = workflow_name_1
        self.assertEqual(workflow.name, workflow_name_1)

        # a workflow name can be set and retrieved through dedicated methods
        workflow_name_2 = spate.utils.random_string()
        workflow.set_name(workflow_name_2)
        self.assertEqual(workflow.get_name(), workflow_name_2)

    def test_job_creation_with_default_parameters (self):
        workflow = spate.new_workflow()

        # paths, template
        job_id_1 = workflow.add_job("c", "d",
            "dummy-template")

        # paths, template, job id
        job_id_2 = workflow.add_job("a", "b",
            "dummy-template", "dummy-id-2")

        # paths, template, job id, variables
        job_id_3 = workflow.add_job("b", "c",
            "dummy-template", "dummy-id-3",
            variable_a = 1, variable_b = 2)

        # the job identifier should be the one we set
        self.assertTrue(job_id_1.startswith("JOB_"))
        self.assertEqual("dummy-id-2", job_id_2)
        self.assertEqual("dummy-id-3", job_id_3)

        # the job template should be the one we set
        self.assertEqual("dummy-template", workflow.get_job_template(job_id_1))
        self.assertEqual("dummy-template", workflow.get_job_template(job_id_2))
        self.assertEqual("dummy-template", workflow.get_job_template(job_id_3))

        # the job variables should be the ones we set
        data = workflow.get_job_data("dummy-id-2")
        self.assertTrue(len(data) == 0)

        data = workflow.get_job_data("dummy-id-3")
        self.assertEqual(data.get("variable_a"), 1)
        self.assertEqual(data.get("variable_b"), 2)

    def test_job_creation_with_explicit_parameters (self):
        workflow = spate.new_workflow()

        job_id = workflow.add_job(
            inputs = "a", outputs = "b",
            template = "dummy-template",
            job_id = "dummy-id",
            variable_a = 1, variable_b = 2)

        # the job identifier should be the one we set
        self.assertEqual("dummy-id", job_id)

        # the job template should be the one we set
        self.assertEqual("dummy-template", workflow.get_job_template(job_id))

        # the job paths should be the ones we set
        inputs, outputs = workflow.get_job_paths(job_id)
        self.assertEqual(inputs, ("a",))
        self.assertEqual(outputs, ("b",))

        # the job variables should be the ones we set
        data = workflow.get_job_data("dummy-id")
        self.assertEqual(data.get("variable_a"), 1)
        self.assertEqual(data.get("variable_b"), 2)

    def test_job_properties_manipulation (self):
        workflow = spate.new_workflow()

        job_id = workflow.add_job(
            inputs = "a", outputs = "b",
            template = "dummy-template",
            job_id = "dummy-id",
            variable_a = 1, variable_b = 2)

        # the job template can be modified at will
        new_template = "new-template"
        workflow.set_job_template(job_id, new_template)
        self.assertEqual(new_template, workflow.get_job_template(job_id))

        # the job data can be modified at will
        new_data = {"variable_c": 3}
        workflow.set_job_data(job_id, **new_data)
        self.assertEqual(new_data, workflow.get_job_data(job_id))

        # however, the data we get out of get_job_data() is a copy
        workflow.get_job_data(job_id)["variable_c"] += 1
        self.assertEqual(new_data, workflow.get_job_data(job_id))

    def test_job_addition_and_deletion (self):
        workflow = spate.new_workflow()

        # a newly created workflow should have no job nor path
        self.assertEqual(workflow.number_of_jobs, 0)
        self.assertEqual(workflow.number_of_paths, 0)

        # add_job() accepts either a string or a
        # list of strings for inputs and outputs
        input_sets = ("a", ("a", "b"), ("a", "b", "c"))
        output_sets = ("x", ("x", "y"), ("x", "y", "z"))
        path_sets = itertools.product(input_sets, output_sets)

        for n, (input_set, output_set) in enumerate(path_sets):
            # after adding this job...
            dummy_job_id = "dummy-%d" % n
            created_job_id = workflow.add_job(
                input_set, output_set,
                job_id = dummy_job_id)

            # we should see the corresponding number of jobs and paths
            self.assertEqual(workflow.number_of_jobs, 1)
            self.assertEqual(workflow.number_of_paths,
                len(input_set) + len(output_set))

            # we should see this job
            self.assertTrue(workflow.has_job(dummy_job_id))
            self.assertTrue(dummy_job_id in workflow)

            # we should see these paths
            for path in tuple(input_set) + tuple(output_set):
                self.assertTrue(workflow.has_path(path))

            # this job should be seen as connected to these paths
            inputs, outputs = workflow.get_job_paths(dummy_job_id)
            self.assertEqual(inputs, tuple(input_set))
            self.assertEqual(outputs, tuple(output_set))

            for path in tuple(input_set):
                upstream, downstream = workflow.get_path_jobs(path)
                self.assertEqual(len(upstream), 0)
                self.assertEqual(downstream, (dummy_job_id,))

            for path in tuple(output_set):
                upstream, downstream = workflow.get_path_jobs(path)
                self.assertEqual(upstream, (dummy_job_id,))
                self.assertEqual(len(downstream), 0)

            # after deleting this job...
            workflow.remove_job(dummy_job_id)

            # we should not see this job
            self.assertFalse(workflow.has_job(dummy_job_id))
            self.assertFalse(dummy_job_id in workflow)

            # we should not see these paths
            for path in tuple(input_set) + tuple(output_set):
                self.assertFalse(workflow.has_path(path))

            # we should have an empty workflow
            self.assertEqual(workflow.number_of_jobs, 0)
            self.assertEqual(workflow.number_of_paths, 0)

    def test_job_batch_addition (self):
        workflow = spate.new_workflow()

        # we add a first normal job
        workflow.add_job('a', 'b', job_id = "normal-job-1")

        # we add a set of normal jobs
        normal_jobs_batch = (
            ("b", "c", "dummy-template", "normal-job-2", None),
            ("c", "d", "dummy-template", "normal-job-3", None),
            ("d", "e", "dummy-template", "normal-job-4", None))

        # we expect the addition of this set of jobs to succeed
        added_jobs = workflow.add_jobs(normal_jobs_batch)

        self.assertEqual(workflow.number_of_jobs, 4)
        self.assertEqual(workflow.number_of_paths, 5)
        self.assertEqual(added_jobs,
            ["normal-job-%d" % i for i in range(2, 5)])
        self.assertEqual(sorted(workflow.list_jobs()),
            ["normal-job-%d" % i for i in range(1, 5)])

        # we then create a batch of other jobs, the two last ones being faulty
        faulty_jobs_batch = (
            ("e", "f", "dummy-template", "normal-job-5", None),
            ("f", "a", "dummy-template", "faulty-job-1", None),
            ("f", "b", "dummy-template", "faulty-job-2", None))

        # we expect the addition of this set of jobs to fail
        with self.assertRaises(spate.SpateException):
            workflow.add_jobs(faulty_jobs_batch)

        # we expect that none of these jobs are left
        self.assertEqual(workflow.number_of_jobs, 4)
        self.assertEqual(workflow.number_of_paths, 5)

        self.assertEqual(sorted(workflow.list_jobs()),
            ["normal-job-%d" % i for i in range(1, 5)])

    def test_job_creation_with_faulty_parameters (self):
        workflow = spate.new_workflow()

        # add_job() only accepts strings for job identifier
        for job_id in (True, 123, 1.23, {}, []):
            with self.assertRaises(ValueError):
                workflow.add_job("x", "y", job_id = job_id)

            self.assertEqual(workflow.number_of_jobs, 0)
            self.assertFalse(job_id in workflow)

        # add_job() only accepts unique job identifiers
        workflow.add_job("a", "b", job_id = "dummy-job-id-1")

        with self.assertRaises(spate.SpateException):
            workflow.add_job("c", "d", job_id = "dummy-job-id-1")

        self.assertEqual(workflow.number_of_jobs, 1)
        workflow.add_job("c", "d", job_id = "dummy-job-id-2")

if (__name__ == "__main__"):
    unittest.main()
