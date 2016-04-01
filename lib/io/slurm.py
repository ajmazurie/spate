
import logging
import os

from .. import errors
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

def process_kwargs (kwargs, **pre_kwargs):
    def _slurm_flag_mapper (flag):
        if (flag in _SBATCH_OPTIONS_WITH_UNDERLINE):
            return flag
        else:
            return _SBATCH_OPTIONS_WITH_DASH.get(flag, flag).replace('_', '-')

    sbatch_kwargs = utils.parse_flags(
        kwargs, pre_kwargs, {}, _slurm_flag_mapper)

    sbatch_args = []
    for k in sorted(sbatch_kwargs):
        v = sbatch_kwargs[k]
        if (v is None):
            sbatch_args.append("#SBATCH --%s" % k)
        else:
            sbatch_args.append("#SBATCH --%s %s" % (k, v))

    return sorted(sbatch_args)

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

    jobs = workflow.list_jobs(
        outdated_only = outdated_only)

    target_fh, is_named_target = utils.stream_writer(target)
    logger.debug("exporting %s to %s" % (workflow, target_fh))

    # write master sbatch script options
    workflow_kwargs = {"job-name": workflow.name}
    for (k, v) in workflow.get_kwargs().iteritems():
        if (k.lower().startswith("_sbatch__")):
            workflow_kwargs[k[9:]] = v

    master_sbatch_args = process_kwargs(sbatch_kwargs, **workflow_kwargs)
    target_fh.write("#!/bin/bash\n%s\n" % '\n'.join(master_sbatch_args))

    # write per-job sbatch subscripts
    job_idx, job_name_to_idx = 1, {}
    for name in jobs:
        body = '\n'.join(utils.dedent_text_block(
            workflow.render_job_content(name)))

        # write job sbatch script options
        job_kwargs = {"job-name": name}
        for (k, v) in workflow.get_job_kwargs(name).iteritems():
            if (k.lower().startswith("_sbatch__")):
                job_kwargs[k[9:]] = v

        job_sbatch_args = process_kwargs(job_kwargs, **job_kwargs)
        body = '\n'.join(job_sbatch_args) + '\n' + body

        # we list all upstream jobs,
        parent_job_names = workflow.get_job_predecessors(name)
        # ignoring these that will be skipped
        # over because they are current
        parent_job_names = filter(
            lambda name: name in job_name_to_idx, parent_job_names)

        if (len(parent_job_names) == 0):
            dependencies = ''
        else:
            job_name_mapper = lambda name: \
                ":${JOB_%d_ID}" % job_name_to_idx[name]
            dependencies = " --dependency=afterok" + ''.join(
                map(job_name_mapper, parent_job_names))

        target_fh.write((
            "\n# %(name)s\n"
            "JOB_%(job_idx)d_ID=$("
            "sbatch%(dependencies)s "
            "<<'EOB_JOB_%(job_idx)d'\n"
            "#!/bin/bash\n%(body)s\n"
            "EOB_JOB_%(job_idx)d\n"
            "); JOB_%(job_idx)d_ID=${JOB_%(job_idx)d_ID##* }\n"
            ) % locals())

        job_name_to_idx[name] = job_idx
        job_idx += 1

    n_jobs = job_idx - 1
    logger.debug("%d jobs exported" % n_jobs)

    if (n_jobs == 0) and (is_named_target):
        logger.debug("removing named output file '%s'" % target)

        target_fh.close()
        os.remove(target)

    return n_jobs
