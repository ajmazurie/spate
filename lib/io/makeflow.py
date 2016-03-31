
import collections
import logging
import os

from .. import errors
import utils

__all__ = (
    "to_makeflow",)

logger = logging.getLogger(__name__)

def to_makeflow (workflow, target, outdated_only = True,
    **makeflow_kwargs):
    """ Export a workflow as a Makeflow script

        Arguments:
            workflow (obj): a workflow object
            target (str or obj): a target filename or file-like object
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported
            **makeflow_kwargs (dict, optional): variables for the Makeflow
                script; these variables will be declared before all jobs

        Returns:
            int: number of jobs exported

        Notes:
        [1] If no job is found in the workflow, no file will be created
        [2] Makeflow requires jobs to have at least one input and one output
            file; a SpateException will be thrown if that is not the case
    """
    utils.ensure_workflow(workflow)

    jobs = workflow.list_jobs(
        outdated_only = outdated_only,
        with_paths = True)

    target_fh, is_named_target = utils.stream_writer(target)
    logger.debug("exporting %s to %s" % (workflow, target_fh))

    # write global variables
    global_kwargs = collections.OrderedDict()
    for (k, v) in workflow.get_kwargs().iteritems():
        global_kwargs[k] = v
    for (k, v) in makeflow_kwargs.iteritems():
        global_kwargs[k] = v

    for (k, v) in global_kwargs.iteritems():
        target_fh.write("%s=%s\n" % (k, v))

    # write jobs
    n_jobs = 0
    for (name, input_paths, output_paths) in jobs:
        if (len(input_paths) == 0):
            raise errors.SpateException(
                "Makeflow requires at least one input per job")

        if (len(output_paths) == 0):
            raise errors.SpateException(
                "Makeflow requires at least one output per job")

        # note: Makeflow only allows a one-line body
        body = utils.flatten_text_block(
            workflow.render_job_content(name))

        target_fh.write("\n# %s\n%s: %s\n\t%s\n\n" % (
            name,
            ' '.join(output_paths),
            ' '.join(input_paths),
            body))

        n_jobs += 1

    logger.debug("%d jobs exported" % n_jobs)

    if (n_jobs == 0) and (is_named_target):
        logger.debug("removing named output file '%s'" % target)

        target_fh.close()
        os.remove(target)

    return n_jobs
