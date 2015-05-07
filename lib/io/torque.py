# Export a workflow as a TORQUE/PBS array

import logging
import os

from .. import errors
from .. import templates
import utils

__all__ = (
    "to_torque_array",)

logger = logging.getLogger(__name__)

_SBATCH_SCRIPT_TEMPLATE = """\
#!/bin/bash
%(qsub_args)s
%(cwd)s
_ALL_JOBS="%(torque_jobs_fn)s"
_CURRENT_JOB="$(awk "NR==${PBS_ARRAYID}" ${_ALL_JOBS})"

echo ${_CURRENT_JOB}
echo

eval ${_CURRENT_JOB}
"""

def to_torque_array (workflow, output_prefix,
    outdated_only = True, **qsub_kwargs):
    """ Export a workflow as a TORQUE/PBS array

        Arguments:
            workflow (object): a workflow object
            output_prefix (str): the prefix for all output files
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported
            **qsub_kwargs (dict, optional): arguments for qsub

        Returns:
            int: number of jobs exported
    """
    utils.ensure_workflow(workflow)

    torque_jobs_fn = output_prefix + ".torque_jobs"
    torque_jobs_fh = open(torque_jobs_fn, "w")

    jobs = workflow.list_jobs(
        outdated_only = outdated_only,
        with_descendants = False)

    n_jobs = 0
    for job_id in jobs:
        body = utils.flatten_text_block(
            templates.render_job(workflow, job_id))

        torque_jobs_fh.write(body + '\n')
        n_jobs += 1

    torque_jobs_fh.close()

    if (n_jobs == 0):
        os.remove(torque_jobs_fn)
        return 0

    qsub_kwargs = utils.parse_flags(
        qsub_kwargs, {
            "N": workflow.name,
            "o": torque_jobs_fn + "_${PBS_JOBID}_${PBS_ARRAYID}.out",
            "e": torque_jobs_fn + "_${PBS_JOBID}_${PBS_ARRAYID}.err",
        }, {
            "t": "1-%d" % n_jobs,
        })

    qsub_kwargs["N"] = qsub_kwargs["N"][:15]  # per QSUB documentation

    # TORQUE/PBS doesn't have a 'cwd' flag as SGE
    # does; here we re-implement it artificially
    if ("cwd" in qsub_kwargs):
        del qsub_kwargs["cwd"]
        cwd = "\ncd ${PBS_O_WORKDIR}"
    else:
        cwd = ''

    qsub_args = []
    for k in sorted(qsub_kwargs):
        v = qsub_kwargs[k]
        if (v is None):
            qsub_args.append("#PBS -%s" % k)
        else:
            qsub_args.append("#PBS -%s %s" % (k, v))

    qsub_args = '\n'.join(qsub_args)

    torque_array_fn = output_prefix + ".torque_array"
    torque_array_fh = open(torque_array_fn, "w")

    torque_array_fh.write(_QSUB_SCRIPT_TEMPLATE % locals())
    torque_array_fh.close()

    return n_jobs
