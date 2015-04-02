
from .. import core
from .. import utils
from .. import templates

import sys
import os

__all__ = (
    "to_terminal",
    "to_shell_script",)

def to_terminal (workflow, outdated_only = True, with_colors = True):
    """ Display jobs in the terminal

        Arguments:
            workflow (object): a workflow object
            outdated_only (boolean, optional): if set to True (default), will
                only print outdated jobs rather than all jobs
            with_colors (boolean, optional): if set to True, will add colors
                to the output on color-capable terminals

        Returns:
            int: number of jobs printed

        Notes:
        [1] For the with_colors argument to work, the Colorama library must be
            installed; see https://pypi.python.org/pypi/colorama
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value for workflow: %s (type %s)" % (
            workflow, type(workflow)))

    try:
        if (with_colors):
            colorama = utils.ensure_module("colorama")
            colorama.init()
            highlight = lambda text: colorama.Style.BRIGHT

        n_jobs = 0
        for job_id in workflow.list_jobs(outdated_only = outdated_only):
            input_paths, output_paths = workflow.job_inputs_and_outputs(job_id)

            for input_path in input_paths:
                if (with_colors):
                    sys.stdout.write("%s< %s%s\n" % (
                        colorama.Style.DIM,
                        input_path,
                        colorama.Style.RESET_ALL))
                else:
                    sys.stdout.write("< %s\n" % input_path)

            if (with_colors):
                sys.stdout.write("%s%s%s\n" % (
                    colorama.Style.BRIGHT,
                    job_id,
                    colorama.Style.RESET_ALL))
            else:
                sys.stdout.write("%s\n" % job_id)

            for output_path in output_paths:
                sys.stdout.write("> %s\n" % output_path)

            sys.stdout.write('\n')
            n_jobs += 1

        sys.stdout.write("total: %d job%s\n" % (
            n_jobs, {True: 's', False: ''}[n_jobs > 1]))

    finally:
        if (with_colors):
            colorama.deinit()

    return n_jobs

def to_shell_script (workflow, filename, outdated_only = True,
    shell = "/bin/bash", *shell_args):
    """ Export a workflow as a shell script

        Arguments:
            workflow (object): a workflow object
            filename (str): the name of the shell script
            outdated_only (boolean, optional): if set to True (default), will
                only export outdated jobs rather than all jobs
            shell (str, optional): path to the shell to use; if none provided
                then /bin/bash is used by default
            *shell_args (str, optional): options for the shell, which will be
                inserted before the jobs

        Returns:
            int: number of jobs exported

        Notes:
        [1] The shell script will run the jobs sequentially; the jobs are
            ordered to ensure that the input paths of any given job have
            been produced by one or more previous jobs, if any
        [2] The mode of the output file will be set to 755 (or rwxr-xr-x)
        [3] It is strongly advised, when using the default /bin/bash shell, to
            use the "set -e" argument ('errexit', see bash documentation) so
            that any job returning a non-zero exit code aborts the workflow
        [4] If no job is found in the workflow, no file will be created

        FIXME: shell_args is not properly handled
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value for workflow: %s (type %s)" % (
            workflow, type(workflow)))

    o = open(filename, "w")
    o.write("#%s\n" % shell.strip())

    if (len(shell_args) > 0):
        o.write('\n')
        for shell_arg in shell_args:
            o.write("%s\n" % str(shell_arg).strip())
        o.write('\n')

    n_jobs = 0
    for job_id in workflow.list_jobs(outdated_only = outdated_only):
        body = utils.dedent_text_block(
            templates.render_job(workflow, job_id),
            ignore_empty_lines = False)

        o.write("\n# %s\n%s\n" % (job_id, '\n'.join(body)))
        n_jobs += 1

    o.close()
    os.chmod(filename, 0755)

    if (n_jobs == 0):
        os.remove(filename)

    return n_jobs
