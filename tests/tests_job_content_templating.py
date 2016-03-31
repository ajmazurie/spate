"""
Test of the job content templating system
"""

import spate

import os
import unittest
import tempfile
import StringIO

_template_engines = (
    spate.default_template_engine,
    spate.mustache_template_engine)

def _dummy_content (engine):
    if (engine == spate.default_template_engine):
        return """\
            INPUTS: ${INPUT0} ${INPUT1} (${INPUTN})
            OUTPUTS: ${OUTPUT0} ${OUTPUT1} (${OUTPUTN})
            variable_1: "${variable_1}"
            variable_2: ${variable_2}
            variable_3: ${variable_3}
            global_variable: ${global_variable}
        """

    if (engine == spate.mustache_template_engine):
        return """\
            INPUTS:{{#INPUTS}} {{.}}{{/INPUTS}} ({{INPUTN}})
            OUTPUTS:{{#OUTPUTS}} {{.}}{{/OUTPUTS}} ({{OUTPUTN}})
            variable_1: "{{variable_1}}"
            variable_2: {{variable_2}}
            variable_3: {{variable_3}}
            global_variable: {{global_variable}}
        """

def _dummy_workflow (content):
    workflow = spate.new_workflow("dummy-workflow")

    workflow.add_job(
        inputs = ("a", "b"),
        outputs = ("c", "d"),
        content = content,
        name = "dummy-job-name",
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

class JobsContentTemplatingTests (unittest.TestCase):

    def test_content_templating (self):
        for template_engine in _template_engines:
            workflow = _dummy_workflow(_dummy_content(template_engine))
            name = next(workflow.list_jobs())

            expected_content = """
                INPUTS: a b (2)
                OUTPUTS: c d (2)
                variable_1: "one"
                variable_2: 2
                variable_3: None
                global_variable: True
                """

            self.assertTrue(_is_same_content(
                expected_content,
                spate.render_job_content(workflow, name, template_engine)))

if (__name__ == "__main__"):
    unittest.main()
