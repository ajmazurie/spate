
from .. import core
from .. import utils

import os

__all__ = (
    "to_makeflow",)

def to_makeflow (workflow, filename, jobs_factory, outdated_only = True,
    **makeflow_kwvars):
    """ Export a workflow as a Makeflow script
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value type for workflow")

    o = open(filename, "w")

    for (k, v) in makeflow_kwvars.iteritems():
        o.write("%s=%s\n" % (k, v))

    n_jobs = 0
    for job in utils.build_jobs(
        workflow, jobs_factory, outdated_only = outdated_only):
        job_id, job_inputs, job_outputs, body, _ = job

        if (len(job_inputs) == 0):
            raise Exception("Makeflow requires at least one input per job")

        if (len(job_outputs) == 0):
            raise Exception("Makeflow requires at least one output per job")

        o.write("\n# %s\n%s: %s\n\t%s\n\n" % (
            job_id,
            ' '.join(job_outputs),
            ' '.join(job_inputs),
            utils.flatten_text_block(body)  # Makeflow only allows a one-line body
        ))
        n_jobs += 1

    o.close()

    if (n_jobs == 0):
        os.remove(filename)

    return n_jobs
