# Export to TORQUE/PBS job scheduler

# https://wikis.nyu.edu/display/NYUHPC/Tutorial+-+Submitting+a+job+using+qsub
# http://docs.adaptivecomputing.com/torque/4-0-2/Content/topics/commands/qsub.htm
# https://acf.ku.edu/wiki/index.php/Cluster_Jobs_Submission_Guide

from .. import core
from .. import utils

import os

__all__ = (
    "to_torque_array",)

def to_torque_array (workflow, filenames_prefix, jobs_factory,
    outdated_only = True, **qsub_kwargs):
    """ Export a workflow as a TORQUE/PBS array, in which jobs will run in parallel
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value type for workflow")

    torque_jobs_fn = filenames_prefix + ".torque_jobs"
    torque_jobs_fh = open(torque_jobs_fn, "w")

    n_jobs = 0
    for job in utils.build_jobs(workflow, jobs_factory,
        outdated_only = outdated_only, with_descendants = False):
        _, _, _, body, _ = job
        torque_jobs_fh.write(utils.flatten_text_block(body) + '\n')
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

    torque_array_fn = filenames_prefix + ".torque_array"
    torque_array_fh = open(torque_array_fn, "w")

    torque_array_fh.write("""\
#!/bin/bash
%(qsub_args)s
%(cwd)s
_ALL_JOBS="%(torque_jobs_fn)s"
_CURRENT_JOB="$(awk "NR==${PBS_ARRAYID}" ${_ALL_JOBS})"

echo ${_CURRENT_JOB}
echo

eval ${_CURRENT_JOB}
""" % locals())

    torque_array_fh.close()
    return n_jobs
