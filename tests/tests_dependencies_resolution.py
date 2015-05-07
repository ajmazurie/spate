""" Tests of the job dependency resolution algorithms

    TODO: test list_jobs() with temporary files
    TODO: test list_jobs() with a pre-generated DAG
"""

import spate

import os
import time
import unittest

class TemporaryFiles:
    def __init__ (self):
        self._current_files = {}
        self._cwd = os.path.dirname(os.path.abspath(__file__))

    def tmp (self, path, wanted):
        path = os.path.join(self._cwd, path)
        exists = os.path.exists(path)

        if (exists and (not wanted)):  # delete the file
            os.remove(path)
            if (path in self._current_files):
                del self._current_files[path]

        elif (wanted):  # create or touch the file
            open(path, 'w').close()
            self._current_files[path] = True

        return path

    def __del__ (self):
        for path in self._current_files:
            os.remove(path)

class DependenciesResolutionTests (unittest.TestCase):

    def test_output_collision (self):
        workflow = spate.new_workflow()

        # all files used for testing should not exist already
        tf = TemporaryFiles()
        _ = lambda path: tf.tmp(path, wanted = False)

        # we should not be able to create two jobs that produces the same path
        with self.assertRaises(spate.SpateException):
            workflow.add_job(_("a"), _("b"), job_id = "dummy-1")
            workflow.add_job(_("c"), _("b"), job_id = "dummy-2")

        # the offending job should have been removed, but not previous ones
        self.assertTrue("dummy-1" in workflow)
        self.assertFalse("dummy-2" in workflow)

        self.assertEqual(workflow.number_of_jobs, 1)
        self.assertEqual(workflow.number_of_paths, 2)

    def test_input_and_output_collision (self):
        workflow = spate.new_workflow()

        # all files used for testing should not exist already
        tf = TemporaryFiles()
        _ = lambda path: tf.tmp(path, wanted = False)

        # we should not be able to create a job
        # whose input and output paths overlap
        with self.assertRaises(spate.SpateException):
            workflow.add_job(
                _("a"), _("b"),
                job_id = "dummy-1")

            workflow.add_job(
                (_("c"), _("d")),
                (_("d"), _("e")),
                job_id = "dummy-2")

        # the offending job should have been removed, but not previous ones
        self.assertTrue("dummy-1" in workflow)
        self.assertFalse("dummy-2" in workflow)

        self.assertEqual(workflow.number_of_jobs, 1)
        self.assertEqual(workflow.number_of_paths, 2)

    def test_dag_enforcement (self):
        workflow = spate.new_workflow()

        # all files used for testing should not exist already
        tf = TemporaryFiles()
        _ = lambda path: tf.tmp(path, wanted = False)

        # we should not be able to create cycles in a workflow
        with self.assertRaises(spate.SpateException):
            workflow.add_job(_("a"), _("b"), job_id = "dummy-1")
            workflow.add_job(_("b"), _("c"), job_id = "dummy-2")
            workflow.add_job(_("c"), _("a"), job_id = "dummy-3")

        # the offending job should have been removed
        self.assertTrue("dummy-1" in workflow)
        self.assertTrue("dummy-2" in workflow)
        self.assertFalse("dummy-3" in workflow)

        self.assertEqual(workflow.number_of_jobs, 2)
        self.assertEqual(workflow.number_of_paths, 3)

    def test_predecessors_and_successors (self):
        workflow = spate.new_workflow()

        # all files used for testing should not exist already
        tf = TemporaryFiles()
        _ = lambda path: tf.tmp(path, wanted = False)

        workflow.add_job(outputs = _("a"), job_id = "dummy-1")
        workflow.add_job(outputs = _("b"), job_id = "dummy-2")
        workflow.add_job(inputs = _("c"), job_id = "dummy-3")
        workflow.add_job(inputs = _("d"), job_id = "dummy-4")
        workflow.add_job(
            (_("a"), _("b")),
            (_("c"), _("d")),
            job_id = "dummy-5")

        self.assertEqual(workflow.get_job_predecessors("dummy-5"),
            ("dummy-1", "dummy-2"))
        self.assertEqual(workflow.get_job_successors("dummy-5"),
            ("dummy-3", "dummy-4"))

    def assertMonotonousIncrease (self, values):
        if (len(values) < 2):
            return
        previous_value = values[0]
        for current_value in values[1:]:
            self.assertTrue(current_value >= previous_value)
            previous_value = current_value

    def test_jobs_ordering_in_simple_chain (self):
        workflow = spate.new_workflow()
        tf = TemporaryFiles()

        # we create a chain of CHAIN_LENGTH jobs that each
        # use the path produced by the previous job
        CHAIN_LENGTH = 3
        for i in range(CHAIN_LENGTH):
            workflow.add_job(
                tf.tmp("dummy_path_%d" % i, False),
                tf.tmp("dummy_path_%d" % (i+1), False),
                _level = i)

            i += 1

        job_level = lambda job_id: workflow.get_job_data(job_id)["_level"]

        # test 1: we create each file in the jobs chain in order
        tf.tmp("dummy_path_0", True)
        for i in range(1, CHAIN_LENGTH + 1):
            # here we create the input path for job i
            tf.tmp("dummy_path_%d" % i, True)

            # asking for outdated jobs should only return downstream jobs
            job_ids = list(workflow.list_jobs(
                outdated_only = True, with_descendants = True))

            self.assertEqual(len(job_ids), CHAIN_LENGTH - i)
            self.assertMonotonousIncrease(
                [job_level(job_id) for job_id in job_ids])

            # while asking for all jobs should return the whole chain
            job_ids = list(workflow.list_jobs(
                outdated_only = False, with_descendants = True))

            self.assertEqual(len(job_ids), CHAIN_LENGTH)
            self.assertMonotonousIncrease(
                [job_level(job_id) for job_id in job_ids])

            # asking for outdated, root jobs should only return one job at most
            job_ids = list(workflow.list_jobs(
                outdated_only = True, with_descendants = False))

            self.assertTrue(len(job_ids) < 2)
            if (len(job_ids) == 1):
                self.assertEqual(job_level(job_ids[0]), i)

            # asking for all root jobs should only return one job
            job_ids = list(workflow.list_jobs(
                outdated_only = False, with_descendants = False))

            self.assertEqual(len(job_ids), 1)
            self.assertEqual(job_level(job_ids[0]), 0)

        # test 2: we go backward and update each input file
        for i in range(CHAIN_LENGTH - 1, 0, -1):
            # we wait a whole second before updating input files,
            # since that's the minimum resolution of os.path.getmtime()
            time.sleep(1)

            # here we update the input path for job i
            tf.tmp("dummy_path_%d" % i, True)

            # asking for outdated jobs should only return downstream jobs
            job_ids = list(workflow.list_jobs(
                outdated_only = True, with_descendants = True))

            self.assertEqual(len(job_ids), CHAIN_LENGTH - i)
            self.assertMonotonousIncrease(
                [job_level(job_id) for job_id in job_ids])
            self.assertEqual(job_level(job_ids[0]), i)

    def test_jobs_ordering_in_ffl (self):
        workflow = spate.new_workflow()
        tf = TemporaryFiles()

        #  a --(dummy-job-1)--> b --(dummy-job-2)--> c --(dummy-job-3)--> d
        #  \___(dummy-job-4)--> e --(dummy-job-5)--> f _/
        workflow.add_job(
            tf.tmp("a", False),
            tf.tmp("b", False),
            job_id = "dummy-job-1",
            _level = 1)

        workflow.add_job(
            tf.tmp("b", False),
            tf.tmp("c", False),
            job_id = "dummy-job-2",
            _level = 2)

        workflow.add_job(
            (tf.tmp("c", False),
             tf.tmp("f", False)),
            tf.tmp("d", False),
            job_id = "dummy-job-3",
            _level = 3)

        workflow.add_job(
            tf.tmp("a", False),
            tf.tmp("e", False),
            job_id = "dummy-job-4",
            _level = 1)

        workflow.add_job(
            tf.tmp("e", False),
            tf.tmp("f", False),
            job_id = "dummy-job-5",
            _level = 2)

        job_to_level = {
            "dummy-job-1": 1,
            "dummy-job-2": 2,
            "dummy-job-3": 3,
            "dummy-job-4": 1,
            "dummy-job-5": 2}

        level_to_jobs = {}
        for job_id, level in job_to_level.iteritems():
            level_to_jobs.setdefault(level, []).append(job_id)

        # for each level,
        for level in sorted(level_to_jobs):
            # we create the input paths for the jobs of that level
            for job_id in level_to_jobs[level]:
                input_paths, _ = workflow.get_job_paths(job_id)
                for path in input_paths:
                    tf.tmp(path, True)

            # we expect the jobs immediately listed
            # as obsolete to be those for this level
            job_ids = list(workflow.list_jobs(
                outdated_only = True, with_descendants = False))

            self.assertEqual(sorted(job_ids), sorted(level_to_jobs[level]))

if (__name__ == "__main__"):
    unittest.main()
