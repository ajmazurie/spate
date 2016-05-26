
import collections
import random
import string
import types

def is_string (obj):
    return isinstance(obj, types.StringTypes)

def is_iterable (obj):
    return (isinstance(obj, collections.Iterable)) and \
           (not isinstance(obj, types.StringTypes))

def is_dict (obj):
    return isinstance(obj, types.DictType)

def is_function (obj):
    return isinstance(obj, types.FunctionType)

def is_class (obj):
    return isinstance(obj, types.ClassType)

def ensure_module (name, url = None):
    try:
        return __import__(name)
    except:
        msg = "library '%s' is required but wasn't found" % name
        if (url is not None):
            msg += " (see %s)" % url
        raise Exception(msg)

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

def random_string (length = 20, characters = string.lowercase):
    return ''.join(random.sample(characters, length))

def cmp_dict (dict1, dict2):
    """ Iterative comparison of two dictionaries, ignoring
        subtypes (e.g., dict and OrderedDict, or tuple and list)
    """
    if (sorted(dict1.keys()) != sorted(dict2.keys())):
        return False

    queue = []
    for key1, value1 in dict1.iteritems():
        value2 = dict2[key1]
        queue.append((value1, value2))

    while (len(queue) > 0):
        value1, value2 = queue.pop(0)

        # value1 and value2 must be dictionaries (or not) together
        if (is_dict(value1) != is_dict(value2)):
            return False

        # if dictionaries,
        elif (is_dict(value1)):
            # their content is iteratively compared
            return cmp_dict(dict1[key1], dict2[key1])

        # value1 and value2 must be iterables (or not) together
        if (is_iterable(value1) != is_iterable(value2)):
            return False

        # if iterables,
        elif (is_iterable(value1)):
            # the length of value1 and value2 must be equal
            if (len(value1) != len(value2)):
                return False

            # their content is added to the queue
            queue.extend(zip(value1, value2))
            continue

        # value1 and value2 must be equal
        if (value1 != value2):
            return False

    return True
