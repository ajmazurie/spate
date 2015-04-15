""" Test of the job templating system

    TODO: test for to_slurm_array()
    TODO: test for to_torque_array()
"""

import spate

import os
import unittest
import tempfile
import StringIO

def _dummy_workflow():
    workflow = spate.new_workflow("dummy-workflow")

    workflow.add_job(
        inputs = ("a", "b"),
        outputs = "c",
        template = "dummy-template",
        job_id = "dummy-job-id",
        variable_1 = "one",
        variable_2 = 2,
        variable_3 = None)

    return workflow

def _dummy_template (engine):
    if (engine == spate.default_engine):
        return """\
            INPUTS: ${INPUT0} ${INPUT1} (${INPUTN})
            OUTPUTS: ${OUTPUT} (${OUTPUTN})
            variable_1: "${variable_1}"
            variable_2: ${variable_2}
            variable_3: ${variable_3}
            global_variable: ${global_variable}
        """

    if (engine == spate.mustache_engine):
        return """\
            INPUTS:{{#INPUTS}} {{.}}{{/INPUTS}} ({{INPUTN}})
            OUTPUTS:{{#OUTPUTS}} {{.}}{{/OUTPUTS}} ({{OUTPUTN}})
            variable_1: "{{variable_1}}"
            variable_2: {{variable_2}}
            variable_3: {{variable_3}}
            global_variable: {{global_variable}}
        """

def _filter_and_strip (lines):
    for line in lines:
        line = line.strip()
        if (line != ''):
            yield line

def _cmp_job_body (expected, existing):
    lines_a = list(_filter_and_strip(expected.splitlines()))
    lines_b = list(_filter_and_strip(existing.splitlines()))

    if (len(lines_a) != len(lines_b)):
        return False

    for line_a, line_b in zip(lines_a, lines_b):
        if (line_a.strip() != line_b.strip()):
            return False

    return True

class JobsTemplatingTests (unittest.TestCase):

    def test_export_to_stream (self):
        EXPECTED_OUTPUT = """\
            < a
            < b
            dummy-job-id
            > c

            total: 1 outdated job (out of 1)
            """

        target = StringIO.StringIO()
        spate.echo(_dummy_workflow(), decorated = False, stream = target)

        self.assertTrue(_cmp_job_body(
            EXPECTED_OUTPUT, target.getvalue()))

    def test_export_to_shell_script (self):
        EXPECTED_OUTPUT = """\
            #!/bin/bash

            set -e

            # dummy-job-id
            INPUTS: a b (2)
            OUTPUTS: c (1)
            variable_1: "one"
            variable_2: 2
            variable_3: None
            global_variable: True
            """

        spate.set_template_variable("global_variable", True)

        for engine in (spate.default_engine, spate.mustache_engine):
            workflow = _dummy_workflow()
            workflow.set_job_template(
                "dummy-job-id", _dummy_template(engine))

            spate.set_template_engine(engine)

            target = StringIO.StringIO()
            spate.to_shell_script(workflow, target,
                shell_args = "set -e")

            self.assertTrue(_cmp_job_body(
                EXPECTED_OUTPUT, target.getvalue()))

    def test_export_to_drake (self):
        EXPECTED_OUTPUT = """\
            ; dummy-job-id
            c <- a, b
                dummy-template
            """

        target = StringIO.StringIO()
        spate.to_drake(_dummy_workflow(), target)

        self.assertTrue(_cmp_job_body(
            EXPECTED_OUTPUT, target.getvalue()))

    def test_export_to_makeflow (self):
        EXPECTED_OUTPUT = """\
            global_variable=True

            # dummy-job-id
            c: a b
                dummy-template
            """

        target = StringIO.StringIO()
        spate.to_makeflow(_dummy_workflow(), target,
            global_variable = True)

        self.assertTrue(_cmp_job_body(
            EXPECTED_OUTPUT, target.getvalue()))

if (__name__ == "__main__"):
    unittest.main()
