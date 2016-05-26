
import itertools
import logging

import errors
import paths
import templating
import utils

import enum
import networkx

__all__ = (
    "new_workflow",
    "JOB_STATUS",
    "PATH_STATUS")

logger = logging.getLogger(__name__)

class JOB_STATUS (enum.Enum):
    CURRENT = 0  # the job is current, and won't run
    OUTDATED = 1  # the job is outdated, and will run

class PATH_STATUS (enum.Enum):
    CURRENT = 0  # the path is current, and won't be updated
    MISSING = 1  # the path is missing
    OUTDATED = 2  # the path is outdated, and will be updated

class _NODE_TYPE (enum.Enum):
    JOB = 0
    PATH = 1

class _workflow:
    """ Simple representation of a file-based data processing workflow
    """
    def __init__ (self, name = None, **kwargs):
        if (name is None):
            name = utils.random_string()

        self._graph = networkx.DiGraph(name = name)
        self._kwargs = kwargs

        logger.debug("created a new workflow with name '%s'" % name)

    def get_name (self):
        """ Return the workflow name

            Arguments:
                none

            Returns:
                str: name of the workflow

            Notes:
            [1] The name of a given workflow object can also be retrieved
                by using the dedicated getter function 'name'
        """
        return self._graph.graph["name"]

    def set_name (self, name):
        """ Set the workflow name

            Arguments:
                name (str): new name for the workflow

            Returns:
                nothing

            Notes:
            [1] The name of a given workflow object can also be modified
                by using the dedicated setter function 'name'
        """
        self._graph.graph["name"] = name
        logger.debug("workflow name set to '%s'" % name)

    name = property(get_name, set_name)

    def _ensure_existing_job (self, name):
        job_node_key = (_NODE_TYPE.JOB, name)
        if (not job_node_key in self._graph):
            raise errors.SpateException("unknown job '%s'" % name)

        return job_node_key

    def _ensure_existing_path (self, path):
        path_node_key = (_NODE_TYPE.PATH, path)
        if (not path_node_key in self._graph):
            raise errors.SpateException("unknown path '%s'" % path)

        return path_node_key

    def add_job (self, inputs = None, outputs = None, content = None,
        name = None, **kwargs):
        """ Add a job to this workflow

            Arguments:
                inputs (list of str, optional): list of paths (either files
                    or directories) this job accepts as input, if any
                outputs (list of str, optional): list of paths (either files
                    or directories) this job accepts as output, if any
                content (str, optional): job content to run
                name (str, optional): unique name for this job
                kwargs (dict, optional): additional variables for this job;
                    these variables will be accessible from the job content

            Returns:
                str: job name

            Notes:
            [1] A single string (rather than a list of strings) can be provided
                for arguments `inputs` and/or `outputs`, if needed
            [2] While `inputs` and `outputs` are both optionals, they are not
                optional simultaneously; i.e., at least one input or one output
                path must be provided or a SpateException will be thrown
            [3] Duplicate paths in either `inputs` or `outputs` will be ignored
            [4] Two jobs cannot have the same path as output; if this situation
                occurs a SpateException will be thrown, and the job ignored
            [5] A job cannot add a cycle in the overall directed acyclic graph
                of jobs and their associated paths; if this situation occurs a
                SpateException will be thrown, and the job ignored
            [6] When multiple jobs must be added, it is suggested to use the
                `add_jobs()` function instead for speed purpose
        """
        return self.add_jobs(((
            inputs, outputs, content, name, kwargs),))[0]

    def add_jobs (self, job_definitions):
        """ Add several jobs to this workflow

            Arguments:
                job_definitions (list of list): list of job definitions, as
                    sub-lists of arguments for the `add_job()` function

            Returns:
                list of str: job names

            Notes:
            [1] `job_definitions` can be a list, iterator, or generator
            [2] The same commands as the ones found in `add_job()` applies for
                each job listed in `job_definitions`
            [3] This function is faster than multiple calls to `add_job()` when
                adding several jobs, since verification of the overall topology
                of the workflow is done only once per jobs set
            [4] This function uses a transaction-like approach: if one of the
                job cannot be added successfully, the whole set is ignored
        """
        if (not utils.is_iterable(job_definitions)):
            raise ValueError(
                "invalid type for 'job_definitions' (must be an iterable)")

        job_names, job_index = [], self.number_of_jobs + 1

        delayed_exception = None
        for job_definition in job_definitions:
            try:
                inputs, outputs, content, name, kwargs = job_definition
            except:
                delayed_exception = ValueError(
                    "invalid job definition in position %d" % len(job_names))
                break

            input_paths = utils.ensure_iterable(inputs)
            output_paths = utils.ensure_iterable(outputs)

            # a default job name is created if none is provided
            if (name is None):
                name = "JOB_%d" % job_index
                job_index += 1

            # constraint: job names must be strings
            elif (not utils.is_string(name)):
                delayed_exception = ValueError(
                    "invalid job name '%s': must be a string" % name)
                break

            if (kwargs is None):
                kwargs = {}

            # constraint: job names must be unique
            job_node_key = (_NODE_TYPE.JOB, name)
            if (job_node_key in self._graph):
                delayed_exception = errors.SpateException(
                    "invalid job name '%s': already taken" % name)
                break

            # constraint: inputs and outputs must not have duplicates
            try:
                utils.ensure_unique(input_paths)
                utils.ensure_unique(output_paths)

            except Exception as e:
                delayed_exception = e
                break

            # constraint: a job must have at least one input or output
            if (len(input_paths) == 0) and (len(output_paths) == 0):
                delayed_exception = errors.SpateException(
                    "invalid job '%s': no input nor output declared" % name)
                break

            self._graph.add_node(
                job_node_key,
                _code = None,
                _argv = None)

            for (n, input_path) in enumerate(input_paths):
                self._graph.add_edge(
                    (_NODE_TYPE.PATH, input_path),
                    job_node_key,
                    _order = n + 1)

            for (n, output_path) in enumerate(output_paths):
                self._graph.add_edge(
                    job_node_key,
                    (_NODE_TYPE.PATH, output_path),
                    _order = n + 1)

            job_names.append(name)

            try:
                self.set_job_content(name, content)
                self.set_job_kwargs(name, **kwargs)

            except Exception as exception:
                delayed_exception = exception
                break

            logger.debug("job '%s' added (inputs: %s; outputs: %s)" % (
                name, ' '.join(input_paths), ' '.join(output_paths)))

        if (delayed_exception is None):
            # constraint: any given path is the product of at most one job
            for (node_type, path) in self._graph.nodes():
                if (node_type != _NODE_TYPE.PATH):
                    continue

                path_node_key = (node_type, path)
                if (self._graph.in_degree(path_node_key) < 2):
                    continue

                producing_job_names = ["'%s'" % name for (_, name) in \
                    self._graph.predecessors(path_node_key)]

                delayed_exception = errors.SpateException(
                    "path '%s' is created by more than one job: %s" % (
                    path, ', '.join(producing_job_names)))
                break

        if (delayed_exception is None):
            # constraint: the workflow must be a directed acyclic graph
            if (not networkx.is_directed_acyclic_graph(self._graph)):
                delayed_exception = errors.SpateException(
                    "unable to add job%s %s without creating cycles" % (
                        's' if (len(job_names) != 1) else '',
                        ', '.join(["'%s'" % name for name in job_names])))

        # if any exception was thrown by one of the job addition,
        # we remove all jobs that were added in this transaction
        if (delayed_exception is not None):
            for name in job_names:
                self.remove_job(name)

            raise delayed_exception

        logger.debug("%d job%s added" % (
            len(job_names), 's' if (len(job_names) != 1) else ''))

        return job_names

    def remove_job (self, name):
        """ Remove an existing job from this workflow

            Arguments:
                name (str): job name

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(name)
        input_paths, output_paths = self.get_job_paths(name)

        # remove the job node itself, then
        self._graph.remove_node(job_node_key)
        logger.debug("job '%s' removed" % name)

        # remove any input or output path
        # that would be left disconnected
        for path in input_paths + output_paths:
            path_node_key = (_NODE_TYPE.PATH, path)
            if (path_node_key in self._graph) and \
               (self._graph.degree(path_node_key) == 0):
                self._graph.remove_node(path_node_key)
                logger.debug("removed orphan path '%s'" % path)

    def has_job (self, name):
        """ Test if a job is part of this workflow

            Arguments:
                name (str): job name

            Returns:
                boolean: True if a job exists with this name, False otherwise
        """
        return (name in self)

    def has_path (self, path):
        """ Test if a path is part of this workflow

            Arguments:
                path (str): path

            Returns:
                boolean: True if this path is used in this workflow,
                    False otherwise
        """
        try:
            self._ensure_existing_path(path)
            return True
        except:
            return False

    def __contains__ (self, name):
        if (not utils.is_string(name)):
            return False

        return ((_NODE_TYPE.JOB, name) in self._graph)

    @property
    def number_of_jobs (self):
        """ Return the number of jobs in this workflow

            Arguments:
                nothing

            Returns:
                int: number of jobs in this workflow

            Notes:
            [1] This function can also be used as a getter
        """
        return len(filter(
            lambda (node_type, node): (node_type == _NODE_TYPE.JOB),
            self._graph.nodes_iter()))

    @property
    def number_of_paths (self):
        """ Return the number of paths in this workflow

            Arguments:
                nothing

            Returns:
                int: number of paths in this workflow

            Notes:
            [1] This function can also be used as a getter
        """
        return len(filter(
            lambda (node_type, node): (node_type == _NODE_TYPE.PATH),
            self._graph.nodes_iter()))

    def list_jobs (self, outdated_only = True, with_descendants = True,
        with_paths = False, with_status = False):
        """ List jobs in this workflow, in the order of their execution

            Arguments:
                outdated_only (boolean, optional): if set to True (default),
                    will return only the jobs that need to be re-run as
                    assessed by the availability and modification time of their
                    input and output paths; if False, all jobs are returned
                with_descendants (boolean, optional): if set to True (default),
                    will return all jobs and their descendants; if False, will
                    return only the parent jobs, with the guarantee that these
                    jobs do not depend on each other
                with_paths (boolean, optional): if set to True, will also
                    return jobs input and output paths
                with_status (boolean, optional): if set to True, will also
                    return a status code for the job, input and output paths

            Yields: either
                str: job name
                str, list of str, list of str: job name, list of input
                    paths, and list of output paths (if with_paths = True)
                str, int: job name and status (if with_status = True)
                str, int, list of [str, int], list of [str, int]: job
                    name and job status, list of input paths and path
                    status, and list of output paths and path status (if
                    with_status = True and with_paths = True)

            Notes:
            [1] The integer returned when using the with_status option are
                taken from the `JOB_STATUS` and `PATH_STATUS` enum sets for
                job and path status, respectively
            [2] A job is flagged as outdated if any of following is true:
                - one of its output path is missing
                - one of its input path is produced by an outdated job
                - one of its input path is newer than one of its output path
        """
        # (1) retrieve paths modification time and jobs execution order
        path_mtime, job_names = {}, []
        for (node_type, node) in networkx.topological_sort(self._graph):
            if (node_type == _NODE_TYPE.PATH):
                path_mtime[node] = paths.path_mtime(node)
            elif (node_type == _NODE_TYPE.JOB):
                job_names.append(node)

        flagged_for_creation_or_update = {}
            # paths that will be (re)generated by another job

        # (2) identify jobs that need to be re-run, either...
        for name in job_names:
            cause_for_execution = {}
            input_paths, output_paths = self.get_job_paths(name)

            for input_path in input_paths:
                # (a) because one of the input will be (re)generated
                if (input_path in flagged_for_creation_or_update):
                    cause_for_execution[input_path] = PATH_STATUS.OUTDATED

            for output_path in output_paths:
                # (b) because one of the output is missing
                if (path_mtime[output_path] is None):
                    cause_for_execution[output_path] = PATH_STATUS.MISSING

            for input_path in input_paths:
                input_mtime = path_mtime[input_path]
                if (input_mtime is None):
                    continue

                for output_path in output_paths:
                    output_mtime = path_mtime[output_path]
                    if (output_mtime is None):
                        continue

                    # (c) because one of the input is newer than one of the output
                    if (input_mtime > output_mtime):
                        cause_for_execution[output_path] = PATH_STATUS.OUTDATED

            # if no cause has been listed at that point, the job is current
            if (len(cause_for_execution) == 0):
                job_status = JOB_STATUS.CURRENT

            # else, the job is outdated and so are its output paths
            else:
                job_status = JOB_STATUS.OUTDATED
                for output_path in output_paths:
                    flagged_for_creation_or_update[output_path] = True

            paths_status = cause_for_execution

            for path in input_paths + output_paths:
                # any non existing path is flagged as missing,
                # replacing prior flagging as outdated (if any)
                if (path_mtime[path] is None):
                    paths_status[path] = PATH_STATUS.MISSING

                # any path not flagged at that point is current
                elif (not path in paths_status):
                    paths_status[path] = PATH_STATUS.CURRENT

            # we skip this job if only outdated jobs are requested
            if (outdated_only) and (job_status == JOB_STATUS.CURRENT):
                continue

            if (not with_descendants):
                depends_on_previous_job = False
                if (outdated_only):
                    # we skip this job if it depends on any other obsolete
                    # job and the user only outdated non-dependent jobs
                    for input_path in input_paths:
                        if (input_path in flagged_for_creation_or_update):
                            depends_on_previous_job = True
                            break
                else:
                    # we skip this job if it depends on any other
                    # job and the user wants all non-dependent jobs
                    depends_on_previous_job = (
                        len(self.get_job_predecessors(name)) > 0)

                if (depends_on_previous_job):
                    continue

            # the user only wants job names
            if (not with_paths) and (not with_status):
                yield name

            # the user wants status but not paths
            elif (with_status) and (not with_paths):
                yield (name, job_status)

            # the user wants paths but not status
            elif (not with_status) and (with_paths):
                yield (name, input_paths, output_paths)

            # the users want both paths and status
            else:
                add_status = lambda path: (path, paths_status[path])
                yield (name, job_status,
                    tuple(map(add_status, input_paths)),
                    tuple(map(add_status, output_paths)))

    def get_job_predecessors (self, name):
        """ Return job(s) upstream of a given job, if any

            Arguments:
                name (str): job name

            Returns:
                list of str: list of job names
        """
        job_node_key = self._ensure_existing_job(name)

        input_node_keys = []
        for (_, input_path) in self._graph.predecessors(job_node_key):
            input_node_key = (_NODE_TYPE.PATH, input_path)
            edge = self._graph[input_node_key][job_node_key]
            input_node_keys.append((edge["_order"], input_node_key))

        input_jobs = []
        for (_, input_node_key) in sorted(input_node_keys):
            for (_, name) in self._graph.predecessors(input_node_key):
                if (not name in input_jobs):
                    input_jobs.append(name)

        return tuple(input_jobs)

    def get_job_successors (self, name):
        """ Return job(s) downstream of a given job, if any

            Arguments:
                name (str): job name

            Returns:
                list of str: list of job names
        """
        job_node_key = self._ensure_existing_job(name)

        output_node_keys = []
        for (_, output_path) in self._graph.successors(job_node_key):
            output_node_key = (_NODE_TYPE.PATH, output_path)
            edge = self._graph[job_node_key][output_node_key]
            output_node_keys.append((edge["_order"], output_node_key))

        output_jobs = []
        for (_, output_node_key) in sorted(output_node_keys):
            for (_, name) in self._graph.successors(output_node_key):
                if (not name in output_jobs):
                    output_jobs.append(name)

        return tuple(output_jobs)

    def get_job_paths (self, name):
        """ Return input and output paths associated with a job, if any

            Arguments:
                name (str): job name

            Returns:
                list of str: list of input paths (or empty list)
                list of str: list of output paths (or empty list)

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(name)

        input_paths = []
        for (_, path) in self._graph.predecessors(job_node_key):
            edge = self._graph[(_NODE_TYPE.PATH, path)][job_node_key]
            input_paths.append((edge["_order"], path))

        output_paths = []
        for (_, path) in self._graph.successors(job_node_key):
            edge = self._graph[job_node_key][(_NODE_TYPE.PATH, path)]
            output_paths.append((edge["_order"], path))

        return (
            tuple([path for (_, path) in sorted(input_paths)]),
            tuple([path for (_, path) in sorted(output_paths)]))

    def get_path_jobs (self, path):
        """ Return upstream and downstream jobs associated with a path, if any

            Arguments:
                path (str): a path

            Returns:
                list of str: list of upstream job names (or empty list)
                list of str: list of downstream job names (or empty list)

            Notes:
            [1] A SpateException will be raised if the path doesn't exist
        """
        path_node_key = self._ensure_existing_path(path)

        upstream_jobs = []
        for (_, name) in self._graph.predecessors(path_node_key):
            upstream_jobs.append(name)

        downstream_jobs = []
        for (_, name) in self._graph.successors(path_node_key):
            downstream_jobs.append(name)

        return (
            tuple(sorted(upstream_jobs)),
            tuple(sorted(downstream_jobs)))

    def get_job_content (self, name):
        """ Return the executable content associated with a job, if any

            Arguments:
                name (str): job name

            Returns:
                str: executable content associated with this job

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(name)
        return self._graph.node[job_node_key]["_content"]

    def set_job_content (self, name, content):
        """ Set or update a job executable content

            Arguments:
                name (str): job name
                content (str): executable content

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(name)

        if (content is not None) and (not utils.is_string(content)):
            raise ValueError("invalid type for content (should be str): %s" %
                type(content))

        self._graph.node[job_node_key]["_content"] = content

    def render_job_content (self, name, template_engine = None):
        return templating.render_job_content(self, name, template_engine)

    def _job_kwargs (self, name):
        job_node_key = self._ensure_existing_job(name)
        return self._graph.node[job_node_key].setdefault("_kwargs", {})

    def set_kwargs (self, **kwargs):
        """ Set keyword arguments for the workflow

            Arguments:
                **kwargs (dict): keyword arguments

            Returns:
                nothing

            Notes:
            [1] Any previous keyword argument is deleted
        """
        self._kwargs = kwargs

    def set_job_kwargs (self, name, **kwargs):
        """ Set keyword arguments for a given job

            Arguments:
                name (str): job name
                **kwargs (dict): keyword arguments

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
            [2] Any previous keyword argument is deleted
        """
        job_node_key = self._ensure_existing_job(name)
        self._graph.node[job_node_key]["_kwargs"] = kwargs

    def set_kwarg (self, key, value):
        """ Set or update a keyword argument for the workflow

            Arguments:
                key: keyword argument name
                value: keyword argument value

            Returns:
                nothing

            Notes:
            [1] Any previous value for this keyword argument is overwritten
        """
        self._kwargs[key] = value

    def set_job_kwarg (self, name, key, value):
        """ Set or update a keyword argument value for a given job

            Arguments:
                name (str): job name
                key: keyword argument name
                value: keyword argument value

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
            [2] Any previous value for this keyword argument is overwritten
        """
        self._job_kwargs(name)[key] = value

    def get_kwargs (self):
        """ Return a copy of the workflow keyword arguments

            Arguments:
                nothing

            Returns:
                dict
        """
        return self._kwargs.copy()

    def get_job_kwargs (self, name):
        """ Return a copy of a job keyword arguments

            Arguments:
                name (str): job name

            Returns:
                dict

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        return self._job_kwargs(name).copy()

    def get_kwarg (self, key, default = None):
        """ Get the value for a given workflow keyword argument

            Arguments:
                key: keyword argument name

            Returns:
                keyword argument value
                default (optional): default keyword argument value

            Notes:
            [1] A default value is returned if the keyword argument is not found
        """
        return self._kwargs.get(key, default)

    def get_job_kwarg (self, name, key, default = None):
        """ Get the value for a given job keyword argument

            Arguments:
                name (str): job name
                key: keyword argument name
                default (optional): default keyword argument value

            Returns:
                keyword argument value

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
            [2] A default value is returned if the keyword argument is not found
        """
        return self._job_kwargs(name).get(key, default)

    def has_kwarg (self, key):
        """ Check if a given workflow keyword argument exists

            Arguments:
                key: keyword argument name

            Returns:
                boolean
        """
        return (key in self._kwargs)

    def has_job_kwarg (self, name, key):
        """ Check if a given job keyword argument exists

            Arguments:
                name (str): job name
                key: keyword argument name

            Returns:
                boolean
        """
        return (key in self._job_kwargs(name))

    def del_kwarg (self, key):
        """ Delete a workflow keyword argument

            Arguments:
                key: keyword argument name

            Notes:
            [1] A KeyError will be raised if the keyword argument is not found
        """
        del self._kwargs[key]

    def del_job_kwarg (self, name, key):
        """ Delete a keyword argument for a given job

            Arguments:
                name (str): job name
                key: keyword argument name

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
            [2] A KeyError will be raised if the keyword argument is not found
        """
        del self._job_kwargs(name)[key]

    def __eq__ (self, obj):
        if (not isinstance(obj, self.__class__)):
            return False

        if (self.name != obj.name):
            return False

        if (self.number_of_jobs != obj.number_of_jobs) or \
           (self.number_of_paths != obj.number_of_paths):
            return False

        list_jobs = lambda obj: sorted(obj.list_jobs(
            outdated_only = False, with_paths = True))

        for (job_name_a, input_paths_a, output_paths_a), \
            (job_name_b, input_paths_b, output_paths_b) in \
            zip(list_jobs(self), list_jobs(obj)):
            if (job_name_a != job_name_b):
                return False

            if (sorted(input_paths_a) != sorted(input_paths_b)) or \
               (sorted(output_paths_a) != sorted(output_paths_b)):
                return False

            if (self.get_job_content(job_name_a) != \
                obj.get_job_content(job_name_b)):
                return False

            if (self.get_job_kwargs(job_name_a) != \
                obj.get_job_kwargs(job_name_b)):
                return False

        return True

    def __ne__ (self, obj):
        return not self.__eq__(obj)

    def __add__ (self, obj):
        if (not isinstance(obj, self.__class__)):
            raise ValueError("cannot add '%s' to %s" % (obj, self))

        merged_workflow = _workflow("%s+%s" % (self.name, obj.name))

        for workflow in (self, obj):
            jobs = workflow.list_jobs(outdated_only = False, with_paths = True)
            for (name, input_paths, output_paths) in jobs:
                merged_workflow.add_job(
                    input_paths, output_paths,
                    "%s:%s" % (workflow.name, name),
                    workflow.get_job_content(name),
                    **workflow.get_job_kwargs(name))

        logger.debug("merged workflow %s with %s" % (self, obj))
        return merged_workflow

    def __str__ (self):
        return "<spate.workflow name:'%s' njobs:%d npaths:%d>" % (
            self.name, self.number_of_jobs, self.number_of_paths)

def new_workflow (name = None, **kwargs):
    """ Create a new workflow object

        Arguments:
            name (str, optional): name of the workflow
            **kwargs (dict): keyword arguments

        Returns:
            object: a workflow object
    """
    return _workflow(name, **kwargs)
