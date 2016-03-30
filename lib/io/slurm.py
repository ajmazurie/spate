
import logging
import os

from .. import errors
from .. import templates
import utils

__all__ = (
    "to_slurm",)

logger = logging.getLogger(__name__)

_SBATCH_OPTIONS_WITH_DASH = {
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
    "x": "exclude"}

_SBATCH_OPTIONS_WITH_UNDERLINE = (
    "cpu_bind",
    "mem_bind")

def _slurm_flag_mapper (flag):
    if (flag in _SBATCH_OPTIONS_WITH_UNDERLINE):
        return flag
    else:
        return _SBATCH_OPTIONS_WITH_DASH.get(flag, flag).replace('_', '-')

def to_slurm (workflow, target, outdated_only = True, **sbatch_kwargs):
    """ Export a workflow as a SLURM sbatch script

        Arguments:
            workflow (object): a workflow object
            target (str or object): either a filename, or a file object open
                in writing mode
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported
            **sbatch_kwargs (dict, optional): arguments for sbatch

        Returns:
            int: number of jobs exported

        Notes:
        [1] If no job is found in the workflow, no file will be created
    """
    utils.ensure_workflow(workflow)

    job_ids = workflow.list_jobs(
        outdated_only = outdated_only)

    target_fh, is_named_target = utils.stream_writer(target)
    logger.debug("exporting %s to %s" % (workflow, target_fh))

    # write master sbatch script options
    sbatch_kwargs = utils.parse_flags(
        sbatch_kwargs,
        {"job-name": workflow.name},
        {},
        _slurm_flag_mapper)

    sbatch_args = []
    for k in sorted(sbatch_kwargs):
        v = sbatch_kwargs[k]
        if (v is None):
            sbatch_args.append("#SBATCH --%s" % k)
        else:
            sbatch_args.append("#SBATCH --%s %s" % (k, v))

    target_fh.write("#!/bin/bash\n%s\n" % '\n'.join(sbatch_args))

    # write per-job sbatch subscripts
    job_idx, job_id_to_idx = 1, {}
    for job_id in job_ids:
        body = '\n'.join(utils.dedent_text_block(
            templates.render_job(workflow, job_id)))

        parent_job_ids = workflow.get_job_predecessors(job_id)
        if (len(parent_job_ids) == 0):
            dependencies = ''
        else:
            job_id_mapper = lambda x: ":${JOB_%d_ID}" % job_id_to_idx[x]
            dependencies = " --dependency=afterok" + ''.join(
                map(job_id_mapper, parent_job_ids))

        target_fh.write((
            "\n# %(job_id)s\n"
            "JOB_%(job_idx)d_ID=$("
            "sbatch%(dependencies)s "
            "<<'EOB'\n#!/bin/bash\n%(body)s\nEOB\n"
            "); JOB_%(job_idx)d_ID=${JOB_%(job_idx)d_ID##* }\n"
            ) % locals())

        job_id_to_idx[job_id] = job_idx
        job_idx += 1

    n_jobs = job_idx - 1
    logger.debug("%d jobs exported" % n_jobs)

    if (n_jobs == 0) and (is_named_target):
        logger.debug("removing named output file '%s'" % target)

        target_fh.close()
        os.remove(target)

    return n_jobs
