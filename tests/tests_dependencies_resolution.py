"""
Tests of the job dependency resolution algorithms

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
        exists = os.path.isfile(path)

        if (exists and (not wanted)):  # delete the file
            os.remove(path)
            if (path in self._current_files):
                del self._current_files[path]

        elif (wanted):  # create or touch the file
            if (exists):
                previous_mtime = os.path.getmtime(path)
            else:
                previous_mtime = None
            while True:
                # we ensure that if we touch the file,
                # its modification time is different
                fh = open(path, 'w')
                fh.write(str(time.time()))
                fh.close()
                current_mtime = os.path.getmtime(path)
                if (current_mtime == previous_mtime):
                    time.sleep(1)
                else:
                    break

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
            workflow.add_job(_("a"), _("b"), name = "dummy-1")
            workflow.add_job(_("c"), _("b"), name = "dummy-2")

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
                name = "dummy-1")

            workflow.add_job(
                (_("c"), _("d")),
                (_("d"), _("e")),
                name = "dummy-2")

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
            workflow.add_job(_("a"), _("b"), name = "dummy-1")
            workflow.add_job(_("b"), _("c"), name = "dummy-2")
            workflow.add_job(_("c"), _("a"), name = "dummy-3")

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

        workflow.add_job(outputs = _("a"), name = "dummy-1")
        workflow.add_job(outputs = _("b"), name = "dummy-2")
        workflow.add_job(inputs = _("c"),  name = "dummy-3")
        workflow.add_job(inputs = _("d"),  name = "dummy-4")
        workflow.add_job(
            (_("a"), _("b")),
            (_("c"), _("d")),
            name = "dummy-5")

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
        # use as input a file produced by the previous job
        CHAIN_LENGTH = 3
        for i in range(CHAIN_LENGTH):
            workflow.add_job(
                tf.tmp("dummy_file_%d" % i, False),
                tf.tmp("dummy_file_%d" % (i+1), False),
                _level = i)
            i += 1

        job_level = lambda name: workflow.get_job_kwarg(name, "_level")

        # test 1: we create each file in the jobs chain in order
        tf.tmp("dummy_file_0", True)
        for i in range(1, CHAIN_LENGTH + 1):
            # here we create the input path for job i
            tf.tmp("dummy_file_%d" % i, True)

            # asking for outdated jobs should only return downstream jobs
            job_names = list(workflow.list_jobs(
                outdated_only = True, with_descendants = True))

            self.assertEqual(len(job_names), CHAIN_LENGTH - i)
            self.assertMonotonousIncrease(map(job_level, job_names))

            # while asking for all jobs should return the whole chain
            job_names = list(workflow.list_jobs(
                outdated_only = False, with_descendants = True))

            self.assertEqual(len(job_names), CHAIN_LENGTH)
            self.assertMonotonousIncrease(map(job_level, job_names))

            # asking for outdated, root jobs should only return one job at most
            job_names = list(workflow.list_jobs(
                outdated_only = True, with_descendants = False))

            self.assertTrue(len(job_names) < 2)
            if (len(job_names) == 1):
                self.assertEqual(job_level(job_names[0]), i)

            # asking for all root jobs should only return one job
            job_names = list(workflow.list_jobs(
                outdated_only = False, with_descendants = False))

            self.assertEqual(len(job_names), 1)
            self.assertEqual(job_level(job_names[0]), 0)

        # test 2: we go backward and update each input file
        for i in range(CHAIN_LENGTH - 1, 0, -1):
            # here we update the input path for job i
            tf.tmp("dummy_file_%d" % i, True)

            # asking for outdated jobs should only return downstream jobs
            job_names = list(workflow.list_jobs(
                outdated_only = True, with_descendants = True))

            self.assertEqual(len(job_names), CHAIN_LENGTH - i)
            self.assertMonotonousIncrease(map(job_level, job_names))
            self.assertEqual(job_level(job_names[0]), i)

            time.sleep(1)

    def test_jobs_ordering_in_ffl (self):
        workflow = spate.new_workflow()
        tf = TemporaryFiles()

        #  a --(dummy-job-1)--> b --(dummy-job-2)--> c --(dummy-job-3)--> d
        #  \___(dummy-job-4)--> e --(dummy-job-5)--> f _/
        workflow.add_job(
            tf.tmp("a", False),
            tf.tmp("b", False),
            name = "dummy-job-1",
            _level = 1)

        workflow.add_job(
            tf.tmp("b", False),
            tf.tmp("c", False),
            name = "dummy-job-2",
            _level = 2)

        workflow.add_job(
            (tf.tmp("c", False),
             tf.tmp("f", False)),
            tf.tmp("d", False),
            name = "dummy-job-3",
            _level = 3)

        workflow.add_job(
            tf.tmp("a", False),
            tf.tmp("e", False),
            name = "dummy-job-4",
            _level = 1)

        workflow.add_job(
            tf.tmp("e", False),
            tf.tmp("f", False),
            name = "dummy-job-5",
            _level = 2)

        job_to_level = {
            "dummy-job-1": 1,
            "dummy-job-2": 2,
            "dummy-job-3": 3,
            "dummy-job-4": 1,
            "dummy-job-5": 2}

        level_to_jobs = {}
        for (name, level) in job_to_level.iteritems():
            level_to_jobs.setdefault(level, []).append(name)

        # for each level,
        for level in sorted(level_to_jobs):
            # we create the input paths for the jobs of that level
            for name in level_to_jobs[level]:
                input_paths, _ = workflow.get_job_paths(name)
                for path in input_paths:
                    tf.tmp(path, True)

            # we expect the jobs immediately listed
            # as obsolete to be those for this level
            job_names = list(workflow.list_jobs(
                outdated_only = True, with_descendants = False))

            self.assertEqual(sorted(job_names), sorted(level_to_jobs[level]))

if (__name__ == "__main__"):
    unittest.main()
