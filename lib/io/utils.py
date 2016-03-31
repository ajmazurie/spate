
import bz2
import collections
import functools
import gzip
import os
import textwrap

from .. import core
from .. import utils

def ensure_workflow (obj):
    if (not isinstance(obj, core._workflow)):
        raise ValueError("invalid value for workflow: %s (type: %s)" % (
            obj, type(obj)))

ensure_module = utils.ensure_module

def stream_reader (source):
    if (source is None):
        return sys.stdin, False

    elif (utils.is_string(source)):
        if (source.lower().endswith(".gz")):
            return gzip.open(source, "rb"), True
        elif (source.lower().endswith(".bz2")):
            return bz2.BZ2File(source, "r"), True
        else:
            return open(source, "rU"), True

    elif (hasattr(source, "read")):
        return source, False

    raise ValueError("invalid source object %s (type: %s)" % (
        source, type(source)))

def stream_writer (target):
    if (target is None):
        return sys.stdout, False

    elif (utils.is_string(target)):
        if (target.lower().endswith(".gz")):
            return gzip.open(target, "wb"), True
        elif (target.lower().endswith(".bz2")):
            return bz2.BZ2File(target, "w"), True
        else:
            return open(target, "w"), True

    elif (hasattr(target, "write")):
        return target, False

    raise ValueError("invalid target object %s (type: %s)" % (
        target, type(target)))

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

def parse_flags (kwargs, pre_kwargs, post_kwargs, mapper):
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
