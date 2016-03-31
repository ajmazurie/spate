
import collections
import logging
import os

from .. import errors
import utils

__all__ = (
    "to_makefile",)

logger = logging.getLogger(__name__)

def to_makefile (workflow, target, outdated_only = True,
    shell = "/bin/bash", **make_kwargs):
    """ Export a workflow as a Makefile script

        Arguments:
            workflow (obj): a workflow object
            target (str or obj): a target filename or file-like object
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported
            **make_kwargs (dict, optional): variables for the Makefile script;
                these variables will be declared before all jobs

        Returns:
            int: number of jobs exported

        Notes:
        [1] If no job is found in the workflow, no file will be created
        [2] GNU Make requires jobs to have at least one output file; a
            SpateException will be thrown if that is not the case
        [3] GNU Make cannot handle file names with space in them; a
            SpateException will be thrown if such a case occurs
    """
    utils.ensure_workflow(workflow)

    jobs = workflow.list_jobs(
        outdated_only = outdated_only,
        with_paths = True)

    target_fh, is_named_target = utils.stream_writer(target)
    logger.debug("exporting %s to %s" % (workflow, target_fh))

    # write global variables
    if (shell is not None):
        target_fh.write("\nSHELL := %s\n" % shell)

    global_kwargs = collections.OrderedDict()
    for (k, v) in workflow.get_kwargs().iteritems():
        global_kwargs[k] = v
    for (k, v) in make_kwargs.iteritems():
        global_kwargs[k] = v

    for (k, v) in global_kwargs.iteritems():
        target_fh.write("%s = %s\n" % (k, v))

    # write jobs
    target_paths, all_paths = collections.OrderedDict(), {}

    n_jobs, job_contents = 0, []
    for (name, input_paths, output_paths) in jobs:
        # ensure that we have at least one output path
        if (len(output_paths) == 0):
            raise errors.SpateException(
                "Make requires at least one output per job")

        # ensure that the paths do not contain spaces in them
        for path in input_paths + output_paths:
            all_paths[path] = True
            if (' ' in path):
                raise errors.SpateException(
                    "Make cannot handle spaces in path names: %s" % path)

        # determine if the output paths are intermediary or not
        for path in output_paths:
            _, downstream_jobs = workflow.get_path_jobs(path)
            if (len(downstream_jobs) == 0):
                target_paths[path] = True

        # Make accepts multi-line job content, but no empty lines
        job_content = utils.dedent_text_block(
            workflow.render_job_content(name),
            ignore_empty_lines = True)

        job_contents.append("\n# %s\n%s: %s\n\t%s\n" % (
            name,
            ' '.join(output_paths),
            ' '.join(input_paths),
            '\n\t'.join(['@' + line for line in job_content])))

        n_jobs += 1

    logger.debug("%d jobs exported" % n_jobs)

    if (n_jobs == 0) and (is_named_target):
        logger.debug("removing named output file '%s'" % target)

        target_fh.close()
        os.remove(target)
        return 0

    # select a unique name for the main target, then write it
    default_target_name = "all"
    main_target_name, main_target_suffix = default_target_name, 0

    while True:
        if (main_target_name in all_paths):
            main_target_suffix += 1
            main_target_name = "%s_%d" % (
                default_target_name, main_target_suffix)
        else:
            break

    target_fh.write("\n%s: %s\n" % (
        main_target_name, ' '.join(target_paths)))

    for job_content in job_contents:
        target_fh.write(job_content)

    return n_jobs
