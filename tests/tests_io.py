""" Test of basic workflow and job creation features
"""

import spate

import os
import itertools
import unittest
import tempfile
import StringIO

def _dummy_json():
    return {
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
                    "dummy_variable_1": 1,
                    "dummy_variable_2": 2
                }
            }
        ]
    }

def _dummy_workflow():
    workflow = spate.new_workflow("dummy-workflow")

    workflow.add_job(
        inputs = ("a", "b"),
        outputs = "c",
        template = "dummy-template",
        job_id = "dummy-id",
        variable_1 = 1,
        variable_2 = 2)

    return workflow

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

if (__name__ == "__main__"):
    unittest.main()
