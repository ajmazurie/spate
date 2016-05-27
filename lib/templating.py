# Template engines to render jobs content

import string

import errors
import utils

__all__ = (
    "set_template_engine",
    "get_template_engine",
    "render_job_content",
    "string_template_engine",
    "mustache_template_engine",)

def _ensure_template_engine (obj):
    if (not utils.is_class(obj)) or \
       (not issubclass(obj, _base_template_engine)):
        raise ValueError(
            "invalid value type for engine: %s (type %s)" % (obj, type(obj)))

_current_template_engine = None

def get_template_engine():
    """ Return the template engine currently set

        Arguments:
            nothing

        Returns:
            obj: template engine class
    """
    return _current_template_engine

def set_template_engine (template_engine):
    """ Set the template engine

        Arguments:
            template_engine (obj): template engine class

        Notes:
        [1] `template_engine` must be a subclass of
            `spate.templates._base_template_engine`
    """
    global _current_template_engine
    if (template_engine is not None):
        _ensure_template_engine(template_engine)
        _current_template_engine = template_engine

def render_job_content (workflow, name, template_engine = None):
    if (template_engine is None):
        template_engine = get_template_engine()
    else:
        _ensure_template_engine(template_engine)

    job_template = workflow.get_job_content(name)
    if (job_template is None):
        return ''

    # set up the job environment
    job_env = {}
    for (k, v) in workflow.get_kwargs().iteritems():
        job_env[k] = v
    for (k, v) in workflow.get_job_kwargs(name).iteritems():
        job_env[k] = v

    job_inputs, job_outputs = workflow.get_job_paths(name)

    for (prefix, paths) in (("INPUT", job_inputs), ("OUTPUT", job_outputs)):
        job_env[prefix + 'S'] = paths
        job_env[prefix + 'N'] = len(paths)
        job_env[prefix] = paths[0] if (len(paths) > 0) else ''

    for (n, input_path) in enumerate(job_inputs):
        job_env["INPUT%d" % n] = input_path

    for (n, output_path) in enumerate(job_outputs):
        job_env["OUTPUT%d" % n] = output_path

    # render the job template
    return template_engine.render(job_template, **job_env)

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

class _base_template_engine:
    def render (self, template, **kwargs):
        return template

class string_template_engine (_base_template_engine):
    """ Python string.Template-based template engine

        Arguments:
            template (str): string.Template-compatible template
            **kwargs (dict, optional): variables passed to the template

        Notes:
        [1] For documentation about the format of these templates, see
            https://docs.python.org/2/library/string.html#template-strings
    """
    @classmethod
    def render (cls, template, **kwargs):
        try:
            return string.Template(template).substitute(kwargs)

        except KeyError as e:
            raise errors.SpateException(
                "Unable to render job content: unknown placeholder %s" % e)

        except Exception as e:
            raise errors.SpateException(
                "Unable to render job content: %s" % e)

class mustache_template_engine (_base_template_engine):
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
        pystache = utils.ensure_module("pystache")

        # the following overrides the HTML tag escaping performed
        # by pystache, since it affects the quotes in job content
        pystache.defaults.TAG_ESCAPE = lambda u: u

        return pystache.render(template, kwargs)

set_template_engine(mustache_template_engine)
