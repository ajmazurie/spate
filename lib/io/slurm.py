# Export a workflow as a SLURM job array

from .. import utils
from .. import templates
from base import _ensure_workflow

import os
import logging

__all__ = (
    "to_slurm_array",)

logger = logging.getLogger(__name__)

_SBATCH_SCRIPT_TEMPLATE = """\
#!/bin/bash
%(sbatch_args)s

_ALL_JOBS="%(slurm_jobs_fn)s"
_CURRENT_JOB="$(awk "NR==${SLURM_ARRAY_TASK_ID}" ${_ALL_JOBS})"

echo ${_CURRENT_JOB}
echo

eval ${_CURRENT_JOB}
"""

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

def to_slurm_array (workflow, output_prefix,
    outdated_only = True, max_jobs_per_array = None, **sbatch_kwargs):
    """ Export a workflow as a SLURM array

        Arguments:
            workflow (object): a workflow object
            output_prefix (str): the prefix for all output files
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported
            max_jobs_per_array (int, optional): if set, will limit the number
                of jobs per array to this value, creating additional arrays if
                needed. Additional arrays will receive a numbered suffix
            **sbatch_kwargs (dict, optional): arguments for sbatch

        Returns:
            int: number of jobs exported

        Notes:
        [1] Job arrays are exported as two files: <output_prefix>.slurm_jobs,
            which contains the job commands, and <output_prefix>.slurm_array,
            which contains the shell script that need to be sent to sbatch
    """
    _ensure_workflow(workflow)

    job_ids = list(workflow.list_jobs(
        outdated_only = outdated_only,
        with_descendants = False))

    n_jobs = len(job_ids)
    if (n_jobs == 0):
        return 0

    def _export_jobs (job_ids, suffix):
        # job definitions
        slurm_jobs_fn = output_prefix + suffix + ".slurm_jobs"
        slurm_jobs_fh = open(slurm_jobs_fn, "w")

        for job_id in job_ids:
            body = utils.flatten_text_block(
                templates.render_job(workflow, job_id))
            slurm_jobs_fh.write(body + '\n')

        slurm_jobs_fh.close()

        # sbatch script
        slurm_array_fn = output_prefix + suffix + ".slurm_array"
        slurm_array_fh = open(slurm_array_fn, "w")

        sbatch_kwargs_ = utils.parse_flags(
            sbatch_kwargs, {
                "job-name": workflow.name,
                "output": slurm_jobs_fn + "_%A_%a.out",
                "error": slurm_jobs_fn + "_%A_%a.err",
            }, {
                "array": "1-%d" % len(job_ids)
            },
            _slurm_flag_mapper)

        sbatch_args = []
        for k in sorted(sbatch_kwargs_):
            v = sbatch_kwargs_[k]
            if (v is None):
                sbatch_args.append("#SBATCH --%s" % k)
            else:
                sbatch_args.append("#SBATCH --%s %s" % (k, v))

        sbatch_args = '\n'.join(sbatch_args)

        slurm_array_fh.write(_SBATCH_SCRIPT_TEMPLATE % locals())
        slurm_array_fh.close()

    if (max_jobs_per_array is not None) and (n_jobs > max_jobs_per_array):
        blocks = list(range(0, n_jobs, max_jobs_per_array))
        suffix_length = len(str(len(blocks)))

        for block_i, block_start in enumerate(blocks):
            _export_jobs(
                job_ids[block_start:block_start+max_jobs_per_array],
                '_' + str(block_i + 1).zfill(suffix_length))
    else:
        _export_jobs(job_ids, '')

    return n_jobs
