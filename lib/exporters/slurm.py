
from .. import core
from .. import utils

import os

__all__ = (
    "to_slurm_array",)

_SBATCH_LONG_OPTIONS = {
    "a": "array",
    "A": "account",
    "B": "extra-node-info",
    "C": "constraint",
    "c": "cpus-per-task",
    "d": "dependency",
    "D": "workdir",
    "e": "error",
    "F": "nodefile",
    "H": "hold",
    "I": "immediate",
    "i": "input",
    "J": "job-name",
    "k": "no-kill",
    "L": "licenses",
    "M": "clusters",
    "m": "distribution",
    "N": "nodes",
    "n": "ntasks",
    "O": "overcommit",
    "o": "output",
    "p": "partition",
    "Q": "quiet",
    "s": "share",
    "S": "core-spec",
    "t": "time",
    "w": "nodelist",
    "x": "exclude",
    }

_SBATCH_OPTIONS_WITH_UNDERLINE = (
    "cpu_bind",
    "mem_bind"
    )

def _slurm_flag_mapper (flag):
    if (not flag in _SBATCH_OPTIONS_WITH_UNDERLINE):
        return _SBATCH_LONG_OPTIONS.get(flag, flag).replace('_', '-')
    else:
        return flag

def to_slurm_array (workflow, filenames_prefix, jobs_factory,
    outdated_only = True, **sbatch_kwargs):
    """ Export a workflow as a SLURM array, in which jobs will run in parallel
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value type for workflow")

    slurm_jobs_fn = filenames_prefix + ".slurm_jobs"
    slurm_jobs_fh = open(slurm_jobs_fn, "w")

    n_jobs = 0
    for job in utils.build_jobs(workflow, jobs_factory,
        outdated_only = outdated_only, with_descendants = False):
        _, _, _, body, _ = job
        slurm_jobs_fh.write(utils.flatten_text_block(body) + '\n')
        n_jobs += 1

    slurm_jobs_fh.close()

    if (n_jobs == 0):
        os.remove(slurm_jobs_fn)
        return 0

    sbatch_kwargs = utils.parse_flags(
        sbatch_kwargs, {
            "job-name": workflow.name,
            "output": slurm_jobs_fn + "_%A_%a.out",
            "error": slurm_jobs_fn + "_%A_%a.err",
        }, {
            "array": "1-%d" % n_jobs
        },
        _slurm_flag_mapper)

    sbatch_args = []
    for k in sorted(sbatch_kwargs):
        v = sbatch_kwargs[k]
        if (v is None):
            sbatch_args.append("#SBATCH --%s" % k)
        else:
            sbatch_args.append("#SBATCH --%s %s" % (k, v))

    sbatch_args = '\n'.join(sbatch_args)

    slurm_array_fn = filenames_prefix + ".slurm_array"
    slurm_array_fh = open(slurm_array_fn, "w")

    slurm_array_fh.write("""\
#!/bin/bash
%(sbatch_args)s

_ALL_JOBS="%(slurm_jobs_fn)s"
_CURRENT_JOB="$(awk "NR==${SLURM_ARRAY_TASK_ID}" ${_ALL_JOBS})"

echo ${_CURRENT_JOB}
echo

eval ${_CURRENT_JOB}
""" % locals())

    slurm_array_fh.close()
    return n_jobs
