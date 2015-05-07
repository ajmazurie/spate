
import logging
import os
import sys

from .. import core
from .. import templates
from .. import utils as coreutils
import utils

import colorama

__all__ = (
    "echo",
    "to_shell_script",)

logger = logging.getLogger(__name__)

def echo (workflow, outdated_only = True, decorated = True, colorized = True,
    with_suffix = False, stream = sys.stdout):
    """ Display jobs in the terminal

        Arguments:
            workflow (object): a workflow object
            outdated_only (boolean, optional): if set to True (default), will
                export outdated jobs only, rather than all jobs in the workflow
            decorated (boolean, optional): if set to True, will highlight
                the job names and dim the input paths on ANSI terminals
            colorized (boolean, optional): if set to True, will add colors
                to the output on ANSI terminals; ignored if decorated = False
            with_suffix (boolean, optional): if set to True, paths and jobs
                names will receive a suffix showing their status, if different
                from current
            stream (file object, optional): file object used for the display;
                defaults to sys.stdout

        Returns:
            int: number of jobs printed

        Notes:
        [1] For the `decorated` or `colorized` arguments to work, Colorama must
            be installed; see https://pypi.python.org/pypi/colorama
    """
    utils.ensure_workflow(workflow)
    stream, _ = utils.stream_writer(stream)

    _TERMINAL_JOB_LINE_SUFFIX = {
        core.JOB_STATUS.CURRENT: '',
        core.JOB_STATUS.OUTDATED: " OUTDATED",
    }

    _TERMINAL_PATH_LINE_SUFFIX = {
        core.PATH_STATUS.CURRENT: '',
        core.PATH_STATUS.OUTDATED: " OUTDATED",
        core.PATH_STATUS.MISSING: " MISSING",
    }

    def job_line (job_id, job_status):
        return "%s%s" % (
            job_id,
            {
                True: _TERMINAL_JOB_LINE_SUFFIX[job_status],
                False: ''
            }[with_suffix])

    def path_line (path, path_status, is_input):
        return "%s %s%s" % (
            {
                True: '<',
                False: '>'
            }[is_input],
            path,
            {
                True: _TERMINAL_PATH_LINE_SUFFIX[path_status],
                False: ''
            }[with_suffix])

    if (not decorated):
        colorized = False

    if (decorated or colorized):
        colorama.init()

        _TERMINAL_JOB_LINE_FGCOLOR = {
            core.JOB_STATUS.CURRENT:   colorama.Fore.GREEN,
            core.JOB_STATUS.OUTDATED:  colorama.Fore.YELLOW,
        }

        _TERMINAL_PATH_LINE_FGCOLOR = {
            core.PATH_STATUS.CURRENT:  colorama.Style.DIM + colorama.Fore.GREEN,
            core.PATH_STATUS.MISSING:  colorama.Style.DIM + colorama.Fore.RED,
            core.PATH_STATUS.OUTDATED: colorama.Style.DIM + colorama.Fore.YELLOW,
        }

        raw_job_line = job_line
        raw_path_line = path_line

        def job_line (job_id, job_status):
            return {
                    True: _TERMINAL_JOB_LINE_FGCOLOR[job_status],
                    False: colorama.Style.BRIGHT,
                }[colorized] + \
                raw_job_line(job_id, job_status) + \
                colorama.Style.RESET_ALL

        def path_line (path, path_status, is_input):
            return {
                    (True, True): _TERMINAL_PATH_LINE_FGCOLOR[path_status],
                    (True, False): _TERMINAL_PATH_LINE_FGCOLOR[path_status],
                    (False, True): colorama.Style.DIM,
                    (False, False): '',
                }[(colorized, is_input)] + \
                raw_path_line(path, path_status, is_input) + \
                colorama.Style.RESET_ALL

    jobs = workflow.list_jobs(
        outdated_only = outdated_only,
        with_status = True,
        with_paths = True)

    try:
        n_jobs = 0
        for ((job_id, job_status), input_paths, output_paths) in jobs:
            for (input_path, path_status) in input_paths:
                stream.write(path_line(input_path, path_status, True) + '\n')

            stream.write(job_line(job_id, job_status) + '\n')

            for (output_path, path_status) in output_paths:
                stream.write(path_line(output_path, path_status, False) + '\n')

            stream.write('\n')
            n_jobs += 1

        if (outdated_only):
            stream.write("total: %d outdated job%s (out of %d)\n" % (
                n_jobs, {True: 's', False: ''}[n_jobs > 1],
                workflow.number_of_jobs))
        else:
            stream.write("total: %d job%s\n" % (
                n_jobs, {True: 's', False: ''}[n_jobs > 1]))

    finally:
        if (decorated or colorized):
            colorama.deinit()

    return n_jobs

def to_shell_script (workflow, target, outdated_only = True,
    shell = "/bin/bash", shell_args = []):
    """ Export a workflow as a shell script

        Arguments:
            workflow (object): a workflow object
            target (str or obj): the name of a shell script, or a file object
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported
            shell (str, optional): path to the shell to use; if none provided
                then /bin/bash is used by default
            shell_args (str or list of str, optional): options for the shell,
                which will be inserted before the jobs

        Returns:
            int: number of jobs exported

        Notes:
        [1] The shell script will run the jobs sequentially; the jobs are
            ordered to ensure that the input paths of any given job have
            been produced by one or more previous jobs, if any
        [2] The mode of the output file will be set to 755 (or rwxr-xr-x) if a
            string is provided for `target`
        [3] It is strongly advised, when using the default /bin/bash shell, to
            use the "set -e" argument ('errexit', see bash documentation) so
            that any job returning a non-zero exit code aborts the workflow
        [4] If no job is found in the workflow, no file will be created
    """
    utils.ensure_workflow(workflow)

    target_fh, is_named_target = utils.stream_writer(target)
    logger.debug("exporting %s to %s" % (workflow, target_fh))

    target_fh.write("#!%s\n" % shell.strip())

    shell_args = coreutils.ensure_iterable(shell_args)
    if (len(shell_args) > 0):
        target_fh.write('\n')
        for shell_arg in shell_args:
            target_fh.write("%s\n" % str(shell_arg).strip())

    n_jobs = 0
    for job_id in workflow.list_jobs(outdated_only = outdated_only):
        body = utils.dedent_text_block(
            templates.render_job(workflow, job_id),
            ignore_empty_lines = False)

        target_fh.write("\n# %s\n%s\n" % (job_id, '\n'.join(body)))
        n_jobs += 1

    logger.debug("%d jobs exported" % n_jobs)

    if (is_named_target):
        target_fh.close()
        os.chmod(target, 0755)

        if (n_jobs == 0):
            logger.debug("removing named output file '%s'" % target)
            os.remove(target)

    return n_jobs
