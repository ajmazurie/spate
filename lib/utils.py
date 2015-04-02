
import os
import collections
import types
import random
import string
import textwrap
import enum

def is_string (obj):
    return isinstance(obj, types.StringTypes)

def is_iterable (obj):
    return (isinstance(obj, collections.Iterable)) and \
           (not isinstance(obj, types.StringTypes))

def is_function (obj):
    return isinstance(obj, types.FunctionType)

def is_class (obj):
    return isinstance(obj, types.ClassType)

def ensure_iterable (obj):
    if (obj is None):
        return []
    elif (is_iterable(obj)):
        return obj
    else:
        return [obj]

def ensure_unique (items):
    seen = {}
    for item in items:
        if (item in seen):
            raise Exception("duplicate value '%s'" % item)
        seen[item] = True

def ensure_module (name):
    try:
        return __import__(name)
    except:
        raise Exception("library '%s' must be installed" % name)

def random_string (length = 20, characters = string.lowercase):
    return ''.join(random.sample(characters, length))

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

def dedent_text_block (text, ignore_empty_lines = False):
    text_ = []
    for line in textwrap.dedent(text).strip().splitlines():
        line = line.rstrip()
        if (ignore_empty_lines) and (line == ''):
            continue
        text_.append(line)

    return text_

def flatten_text_block (text):
    text_ = []
    for line in text.splitlines():
        line = line.strip()
        if (line == ''):
            continue
        text_.append(line)

    return '; '.join(text_)

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

class PATH_TYPE (enum.Enum):
    UNKNOWN = -1
    MISSING = 0
    FILE = 1
    DIRECTORY = 2

def path_type (path):
    if (not os.path.exists(path)):
        return PATH_TYPE.MISSING
    elif (os.path.isfile(path)):
        return PATH_TYPE.FILE
    elif (os.path.isdir(path)):
        return PATH_TYPE.DIRECTORY
    else:
        return PATH_TYPE.UNKNOWN

def path_mtime (path):
    path_type_ = path_type(path)

    if (path_type_ == PATH_TYPE.MISSING):
        return None

    elif (path_type_ == PATH_TYPE.UNKNOWN):
        return None

    elif (path_type_ == PATH_TYPE.FILE):
        return os.path.getmtime(path)

    elif (path_type_ == PATH_TYPE.DIRECTORY):
        latest_mtime = 0
        for (top_path, dir_names, file_names) in os.walk(path, followlinks = True):
            for fn in file_names:
                if (fn.startswith('.')):
                    continue

                mtime = os.path.getmtime(os.path.join(top_path, fn))
                if (mtime > latest_mtime):
                    latest_mtime = mtime

        return latest_mtime

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

def build_jobs (workflow, jobs_factory, **kwargs):
    for job_id in workflow.list_jobs(**kwargs):
        job_inputs, job_outputs = workflow.job_inputs_and_outputs(job_id)
        job_data = workflow.job_data(job_id)

        job_body = jobs_factory(job_id, job_inputs, job_outputs, job_data)
        if (job_body is None):
            continue

        yield (job_id, job_inputs, job_outputs, job_body, job_data)

def parse_flags (kwargs, pre_kwargs = None, post_kwargs = None, mapper = None):
    kwargs_ = {}
    for (k, v) in pre_kwargs.iteritems():
        kwargs_[k] = v

    for (k, v) in kwargs.iteritems():
        if (v is None):
            continue
        if (mapper is not None):
            k = mapper(k)
        if (isinstance(v, bool)):
            if (v == True):
                kwargs_[k] = None
            elif (v == False):
                continue
        else:
            kwargs_[k] = v

    for (k, v) in post_kwargs.iteritems():
        kwargs_[k] = v

    return kwargs_
