# Template engines to render jobs code

import string

import core
import utils

__all__ = (
    "set_template_engine",
    "get_template_engine",
    "set_template_variable",
    "set_template_variables",
    "get_template_variable",
    "del_template_variable",
    "render_job",
    "default_engine",
    "mustache_engine",)

def _ensure_template_engine (obj):
    if (not utils.is_class(obj)) or \
       (not issubclass(obj, _base_template_engine)):
        raise ValueError(
            "invalid value type for engine: %s (type %s)" % (obj, type(obj)))

_current_template_engine = None

def get_template_engine():
    """ Return the template engine for all jobs

        Arguments:
            nothing

        Returns:
            obj: template engine class
    """
    return _current_template_engine

def set_template_engine (template_engine):
    """ Set the template engine for all jobs

        Arguments:
            template_engine (obj): template engine class

        Notes:
        [1] `template_engine` must be a subclass of
            `spate.templates._base_template_engine`
    """
    global _current_template_engine
    if (template_engine is None):
        _current_template_engine = default_engine
    else:
        _ensure_template_engine(template_engine)
        _current_template_engine = template_engine

_global_template_variables = {}

def set_template_variable (name, value):
    """ Set a variable that all job templates can access

        Arguments:
            name (str): name of the variable
            value (obj): value for this variable

        Returns:
            nothing

        Notes:
        [1] This variable will be overwritten by an existing job-specific
            variable with the same name, if any
    """
    if (not utils.is_string(name)):
        raise ValueError(
            "invalid type for name (should be str): %s" % type(name))

    _global_template_variables[name] = value

def set_template_variables (**kwargs):
    """ Set one or more variables that all job templates can access

        Arguments:
            **kwargs (dict): keys and values

        Returns:
            nothing

        Notes:
        [1] These variables will be overwritten by existing job-specific
            variables with the same name, if any
    """
    for k, v in kwargs.iteritems():
        set_template_variable(k, v)

def get_template_variable (name):
    """ Return the value of a variable set for all job templates

        Arguments:
            name (str): name of the variable

        Returns:
            obj: value for this variable
    """
    return _global_template_variables[name]

def del_template_variable (name):
    """ Delete a variable from those set for all job templates

        Arguments:
            name (str): name of the variable

        Returns:
            nothing
    """
    del _global_template_variables[name]

def render_job (workflow, job_id, template_engine = None):
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value for workflow: %s (type %s)" % (
            workflow, type(workflow)))

    if (template_engine is None):
        template_engine = get_template_engine()
    else:
        _ensure_template_engine(template_engine)

    job_template = workflow.get_job_template(job_id)

    # set up the job environment
    job_env = {}
    for k, v in _global_template_variables.iteritems():
        job_env[k] = v
    for k, v in workflow.get_job_data(job_id).iteritems():
        job_env[k] = v

    job_inputs, job_outputs = workflow.get_job_paths(job_id)

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

        # the following overrides the HTML tag escaping performed by
        # pystache, since it affects the quotes in job body content
        pystache.defaults.TAG_ESCAPE = lambda u: u

        return pystache.render(template, kwargs)

set_template_engine(default_engine)
