
from .. import utils
from .. import templates
from base import _ensure_workflow, _stream_writer

import os
import logging

__all__ = (
    "to_drake",)

logger = logging.getLogger(__name__)

def to_drake (workflow, target, outdated_only = True):
    """ Export a workflow as a Drake script

        Arguments:
            workflow (object): a workflow object
            target (str or object): either a filename, or a file object open
                in writing mode
            outdated_only (boolean, optional): if set to True (default), will
                only export outdated jobs rather than all existing jobs

        Returns:
            int: number of jobs exported

        Notes:
        [1] If no job is found in the workflow, no file will be created
    """
    _ensure_workflow(workflow)

    jobs = workflow.list_jobs(
        outdated_only = outdated_only,
        with_paths = True)

    o_fh = _stream_writer(target)
    logger.debug("exporting %s to %s" % (workflow, o_fh))

    n_jobs = 0
    for (job_id, input_paths, output_paths) in jobs:
        # note: Drake doesn't allow empty lines
        body = '\n\t'.join(utils.dedent_text_block(
            templates.render_job(workflow, job_id),
            ignore_empty_lines = True))

        o_fh.write("; %s\n%s <- %s\n\t%s\n\n" % (
            job_id,
            ', '.join(output_paths),
            ', '.join(input_paths),
            body))
        n_jobs += 1

    logger.debug("%d jobs exported" % n_jobs)

    if (n_jobs == 0) and (utils.is_string(target)):
        logger.debug("removing named output file '%s'" % target)

        o_fh.close()
        os.remove(target)

    return n_jobs
