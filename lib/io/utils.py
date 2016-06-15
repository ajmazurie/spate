
import bz2
import collections
import functools
import gzip
import os
import pipes
import re
import sys

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
    text_, min_n_leading_whitespaces = [], sys.maxint

    for line in text.splitlines():
        line = line.rstrip()
        if (line == ''):
            if (not ignore_empty_lines):
                text_.append(line)
            continue

        min_n_leading_whitespaces = min(
            min_n_leading_whitespaces,
            len(line) - len(line.lstrip()))

        text_.append(line)

    # we remove the first and last empty lines, if any
    if (len(text_) > 0):
        if (text_[0] == ''):
            text_ = text_[1:]
        if (text_[-1] == ''):
            text_ = text_[:-1]

    return map(lambda line: line[min_n_leading_whitespaces:], text_)

def flatten_text_block (text):
    text_ = []
    for line in text.splitlines():
        line = line.strip()
        if (line == ''):
            continue
        text_.append(line)

    return '; '.join(text_)

def escape_quotes (text):
    return pipes.quote(text)

def merge_kwargs (kwargs, pre_kwargs, post_kwargs, mapper = None):
    kwargs_ = {}
    def add_kwarg (k, v):
        # we ignore None and empty strings
        if (v is None) or (str(v).strip() == ''):
            return
        # we ignore False
        if (isinstance(v, bool)) and (not v):
            return

        if (mapper is not None):
            k = mapper(k)
        kwargs_[k] = v

    if (pre_kwargs is not None):
        for (k, v) in pre_kwargs.iteritems():
            add_kwarg(k, v)

    if (kwargs is not None):
        for (k, v) in kwargs.iteritems():
            add_kwarg(k, v)

    if (post_kwargs is not None):
        for (k, v) in post_kwargs.iteritems():
            add_kwarg(k, v)

    return kwargs_

def filter_kwargs (kwargs, prefix):
    pattern = re.compile("_+%s_+(.+)" % prefix, re.IGNORECASE)
    for (k, v) in kwargs.iteritems():
        m = pattern.match(k)
        if (m is None):
            continue

        yield (m.group(1), v)
