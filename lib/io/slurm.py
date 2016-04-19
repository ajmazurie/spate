
import logging
import os

from .. import errors
import utils

__all__ = (
    "to_slurm",)

logger = logging.getLogger(__name__)

_SBATCH_OPTIONS_WITH_SHORTCUT = {
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

def process_sbatch_kwargs (kwargs, pre_kwargs = None):
    def _slurm_flag_mapper (flag):
        if (flag in _SBATCH_OPTIONS_WITH_UNDERLINE):
            return flag
        else:
            flag = _SBATCH_OPTIONS_WITH_SHORTCUT.get(flag, flag)
            return flag.replace('_', '-')

    sbatch_kwargs = utils.merge_kwargs(
        kwargs, pre_kwargs, None, _slurm_flag_mapper)

    sbatch_args = []
    for k in sorted(sbatch_kwargs):
        v = sbatch_kwargs[k]
        if (v is True):
            sbatch_args.append("#SBATCH --%s" % k)
        else:
            sbatch_args.append("#SBATCH --%s %s" % (k,
                utils.escape_quotes(str(v))))

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
        [2] Any workflow keyword argument starting with '_sbatch_' (case
            insensitive) will be interpreted as a parameter for SBATCH; e.g.,
            _sbatch_partition would set the value for the --partition parameter
        [3] Similarily, any job keyword argument starting with '_sbatch_' will
            be interpreted as a parameter for SBATCH for that particular job
        [4] The **sbatch_kwargs provided when calling this function will
            overwrite any workflow keyword argument for SBATCH
    """
    utils.ensure_workflow(workflow)

    jobs = workflow.list_jobs(
        outdated_only = outdated_only)

    target_fh, is_named_target = utils.stream_writer(target)
    logger.debug("exporting %s to %s" % (workflow, target_fh))

    extract_sbatch_options = lambda kwargs: \
        utils.filter_kwargs(kwargs, "sbatch")

    # write master sbatch script options
    workflow_kwargs = workflow.get_kwargs()
    workflow_sbatch_kwargs = {"J": workflow.name}
    for (k, v) in extract_sbatch_options(workflow_kwargs):
        workflow_sbatch_kwargs[k] = v

    master_sbatch_args = process_sbatch_kwargs(
        sbatch_kwargs, workflow_sbatch_kwargs)

    target_fh.write("#!/bin/bash\n%s\n" % '\n'.join(master_sbatch_args))

    # write per-job sbatch subscripts
    job_idx, job_name_to_idx = 1, {}
    for name in jobs:
        body = '\n'.join(utils.dedent_text_block(
            workflow.render_job_content(name)))

        # write job sbatch script options
        job_kwargs = workflow.get_job_kwargs(name)
        job_sbatch_kwargs = {"J": name}
        for (k, v) in extract_sbatch_options(job_kwargs):
            job_sbatch_kwargs[k] = v

        job_sbatch_args = process_sbatch_kwargs(job_sbatch_kwargs)
        body = '\n'.join(job_sbatch_args) + '\n\n' + body

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
