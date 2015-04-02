
from .. import core
from .. import utils

import os

__all__ = (
    "to_shell_script",)

def to_shell_script (workflow, filename, jobs_factory, outdated_only = True,
    shell = "/bin/bash"):
    """ Export a workflow as a shell script, in which jobs will run sequentially
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value type for workflow")

    o = open(filename, "w")
    o.write("#%s\n" % shell.strip())

    n_jobs = 0
    for job in utils.build_jobs(
        workflow, jobs_factory, outdated_only = outdated_only):
        job_id, _, _, body, _ = job

        o.write("\n# %s\n%s\n" % (
            job_id,
            '\n'.join(utils.dedent_text_block(body))
        ))
        n_jobs += 1

    o.close()
    os.chmod(filename, 0755)

    if (n_jobs == 0):
        os.remove(filename)

    return n_jobs
