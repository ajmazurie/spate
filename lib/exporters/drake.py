
from .. import utils

import os

__all__ = (
    "to_drake",)

def to_drake (workflow, filename, jobs_factory, outdated_only = True):
    """ Export a workflow as a Drake script
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value type for workflow")

    o = open(filename, "w")

    n_jobs = 0
    for job in utils.build_jobs(
        workflow, jobs_factory, outdated_only = outdated_only):
        job_id, job_inputs, job_outputs, body, _ = job

        o.write("; %s\n%s <- %s\n\t%s\n\n" % (
            job_id,
            ', '.join(job_outputs),
            ', '.join(job_inputs),
            '\n\t'.join(utils.dedent_text_block(body, True))
        ))
        n_jobs += 1

    o.close()

    if (n_jobs == 0):
        os.remove(filename)

    return n_jobs
