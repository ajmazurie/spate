# Template engines to render jobs code

import core
import utils

import string

__all__ = (
    "set_template_engine",
    "get_template_engine",
    "set_template_variable",
    "get_template_variable",
    "del_template_variable",
    "render_job",
    "default_engine",
    "mustache_engine",)

def _ensure_template_engine (obj):
    if (not utils.is_class(obj)) or (not issubclass(obj, _base_template_engine)):
        raise ValueError(
            "invalid value type for engine: %s (type %s)" % (obj, type(obj)))

_current_template_engine = None

def set_template_engine (template_engine):
    global _current_template_engine
    _ensure_template_engine(template_engine)
    _current_template_engine = template_engine

def get_template_engine():
    return _current_template_engine

_global_template_variables = {}

def set_template_variable (**kwargs):
    """ Set one or more variables for all job templates

        Notes:
        [1] These variables may be overwritten by job-specific ones
    """
    for k, v in kwargs.iteritems():
        _global_template_variables[k] = v

def get_template_variable (name):
    """ Return the value of a variable that has been set for all job templates
    """
    return _global_template_variables[name]

def del_template_variable (name):
    """ Delete a variable from those accessible to all job templates
    """
    del _global_template_variables[name]

def render_job (workflow, job_id, template_engine = None):
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value type for workflow: %s (type %s)" % (
            workflow, type(workflow)))

    if (template_engine is None):
        template_engine = get_template_engine()
    else:
        _ensure_template_engine(template_engine)

    job_template = workflow.job_template(job_id)

    # set up the job environment
    job_env = {k: v for k, v in _global_template_variables.iteritems()}
    for k, v in workflow.job_data(job_id).iteritems():
        job_env[k] = v

    job_inputs, job_outputs = workflow.job_inputs_and_outputs(job_id)

    for (prefix, paths) in (("INPUT", job_inputs), ("OUTPUT", job_outputs)):
        job_env[prefix + 'S'] = paths
        job_env[prefix + 'N'] = len(paths)
        job_env[prefix] = paths[0] if (len(paths) > 0) else None

    for n, input_path in enumerate(job_inputs):
        job_env["INPUT%d" % n] = input_path

    for n, output_path in enumerate(job_outputs):
        job_env["OUTPUT%d" % n] = output_path

    # render the job template
    return template_engine.render(job_template, **job_env)

class _base_template_engine:
    def render (self, template, **kwargs):
        return template

    default_template = ''

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

class default_engine (_base_template_engine):
    """ Default template engine, based on the Python string.Template class

        Arguments:
            template (str, optional): string.Template-compatible template
            **kwargs (dict, optional): variables passed to the template

        Notes:
        [1] For documentation about the format of these templates, see
            https://docs.python.org/2/library/string.html#template-strings
    """
    @classmethod
    def render (cls, template = None, **kwargs):
        if (template is None):
            template = '\n'.join([
                "touch \"$OUTPUT%d\"" % n for n in range(kwargs["OUTPUTN"])
            ])

        return string.Template(template).safe_substitute(kwargs)

class mustache_engine (_base_template_engine):
    """ Mustache-based template engine

        Arguments:
            template (str): Mustache-compatible template
            **kwargs (dict, optional): variables passed to the template

        Returns:
            str: rendered job code

        Notes:
        [1] Requires the Pystache library to be installed; see
            https://github.com/defunkt/pystache
        [2] For documentation about the format of these templates, see
            http://mustache.github.io/
    """
    @classmethod
    def render (cls, template, **kwargs):
        if (template is None):
            template = "{{#OUTPUTS}}touch \"{{.}}\"\n{{/OUTPUTS}}"

        pystache = utils.ensure_module("pystache")
        return pystache.render(template, kwargs)

set_template_engine(default_engine)
