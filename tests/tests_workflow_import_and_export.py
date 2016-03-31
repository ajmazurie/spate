"""
Test of workflow import and export features
"""

import spate

import os
import itertools
import unittest
import tempfile
import StringIO

def _dummy_workflow():
    workflow = spate.new_workflow("dummy-workflow")

    workflow.add_job(
        inputs = ("a", "b"),
        outputs = ("c", "d"),
        content = "dummy-content",
        name = "dummy-job-name-1",
        variable_1 = "one",
        variable_2 = 2,
        variable_3 = None)

    workflow.add_job(
        inputs = ("c", "d"),
        outputs = ("e", "f"),
        content = "dummy-content",
        name = "dummy-job-name-2",
        variable_1 = "one",
        variable_2 = 2,
        variable_3 = None)

    workflow.set_kwarg("global_variable", True)

    return workflow

def _filter_and_strip (lines):
    for line in lines:
        line = line.strip()
        if (line != ''):
            yield line

def _is_same_content (expected, existing):
    lines_a = list(_filter_and_strip(expected.splitlines()))
    lines_b = list(_filter_and_strip(existing.splitlines()))

    if (len(lines_a) != len(lines_b)):
        return False

    for line_a, line_b in zip(lines_a, lines_b):
        if (line_a.strip() != line_b.strip()):
            return False

    return True

class IOTests (unittest.TestCase):

    def test_load_save (self):
        workflow = _dummy_workflow()
        targets = []

        named_targets_format = ("yaml", "json")
        named_targets_suffix = ('', ".gz", ".bz2")

        for (named_target_format, named_target_suffix) in \
            itertools.product(named_targets_format, named_targets_suffix):
            targets.append(
                os.path.join(tempfile.gettempdir(), "spate_test.%s%s" % (
                    named_target_format, named_target_suffix)))

        for target in targets:
            spate.save(workflow, target)
            workflow_ = spate.load(target)

            self.assertEqual(workflow, workflow_)

    def test_echo (self):
        EXPECTED_OUTPUT = """\
            < a
            < b
            dummy-job-name-1
            > c
            > d

            < c
            < d
            dummy-job-name-2
            > e
            > f

            total: 2 outdated jobs (out of 2)
            """

        target = StringIO.StringIO()
        spate.echo(_dummy_workflow(),
            decorated = False, stream = target)

        self.assertTrue(_is_same_content(
            EXPECTED_OUTPUT, target.getvalue()))

    def test_export_to_shell_script (self):
        EXPECTED_OUTPUT = """\
            #!/bin/bash

            set -e

            # dummy-job-name-1
            dummy-content

            # dummy-job-name-2
            dummy-content
            """

        target = StringIO.StringIO()
        spate.to_shell_script(_dummy_workflow(), target,
            shell_args = "set -e")

        self.assertTrue(_is_same_content(
            EXPECTED_OUTPUT, target.getvalue()))

    def test_export_to_makefile (self):
        EXPECTED_OUTPUT = """\
            SHELL := /bin/bash
            global_variable = True

            all: e f

            # dummy-job-name-1
            c d: a b
                @dummy-content

            # dummy-job-name-2
            e f: c d
                @dummy-content
            """

        target = StringIO.StringIO()
        spate.to_makefile(_dummy_workflow(), target,
            shell = "/bin/bash",
            global_variable = True)

        self.assertTrue(_is_same_content(
            EXPECTED_OUTPUT, target.getvalue()))

    def test_export_to_drake (self):
        EXPECTED_OUTPUT = """\
            ; dummy-job-name-1
            c, d <- a, b
                dummy-content

            ; dummy-job-name-2
            e, f <- c, d
                dummy-content
            """

        target = StringIO.StringIO()
        spate.to_drake(_dummy_workflow(), target)

        self.assertTrue(_is_same_content(
            EXPECTED_OUTPUT, target.getvalue()))

    def test_export_to_makeflow (self):
        EXPECTED_OUTPUT = """\
            global_variable=True
            global_variable_2=False

            # dummy-job-name-1
            c d: a b
                dummy-content

            # dummy-job-name-2
            e f: c d
                dummy-content
            """

        target = StringIO.StringIO()
        spate.to_makeflow(_dummy_workflow(), target,
            global_variable_2 = False)

        self.assertTrue(_is_same_content(
            EXPECTED_OUTPUT, target.getvalue()))

if (__name__ == "__main__"):
    unittest.main()
