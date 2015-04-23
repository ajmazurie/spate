# Implementation of a file-based data processing workflow

import utils
import errors

import enum
import itertools
import logging

import networkx

__all__ = (
    "new_workflow",
    "JOB_STATUS",
    "PATH_STATUS")

logger = logging.getLogger(__name__)

class JOB_STATUS (enum.Enum):
    CURRENT = 0  # the job is current and not planned to be updated
    OUTDATED = 1  # the job is planned to be updated

class PATH_STATUS (enum.Enum):
    CURRENT = 0  # the path is current and not planned to be updated
    MISSING = 1  # the path is missing
    OUTDATED = 2  # the path is planned to be updated

class _NODE_TYPE (enum.Enum):
    JOB = 0
    PATH = 1

class _workflow:
    """ Simple representation of a file-based data processing workflow
    """
    def __init__ (self, name = None):
        if (name is None):
            name = utils.random_string()

        self._graph = networkx.DiGraph(name = name)
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

    def _ensure_existing_job (self, job_id):
        job_node_key = (_NODE_TYPE.JOB, job_id)
        if (not job_node_key in self._graph):
            raise errors.SpateException("unknown job '%s'" % job_id)

        return job_node_key

    def add_job (self, inputs = None, outputs = None, template = None,
        job_id = None, **job_data):
        """ Add a job to this workflow

            Arguments:
                inputs (list of str, optional): list of paths (either files
                    or directories) this job accepts as input, if any
                outputs (list of str, optional): list of paths (either files
                    or directories) this job accepts as output, if any
                template (str, optional): template for the job code
                job_id (str, optional): unique identifier for this job
                job_data (dict, optional): additional variables for this job;
                    these variables will be accessible from the job template

            Returns:
                str: job identifier

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
            inputs, outputs, template, job_id, job_data),))[0]

    def add_jobs (self, job_definitions):
        """ Add several jobs to this workflow

            Arguments:
                job_definitions (list of list): list of job definitions, as
                    sub-lists of arguments for the `add_job()` function

            Returns:
                list of str: job identifiers

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

        job_ids, delayed_exception = [], None
        for job_definition in job_definitions:
            try:
                inputs, outputs, template, job_id, job_data = job_definition
            except:
                delayed_exception = ValueError(
                    "invalid job definition in position %d" % len(job_ids))
                break

            input_paths = utils.ensure_iterable(inputs)
            output_paths = utils.ensure_iterable(outputs)

            # a default job identifier is created if none is provided
            if (job_id is None):
                job_id = "JOB_%d" % (len(filter(
                    lambda (node_type, node): (node_type == _NODE_TYPE.JOB),
                    self._graph.nodes_iter())) + 1)

            # constraint: job identifiers must be strings
            elif (not utils.is_string(job_id)):
                delayed_exception = ValueError(
                    "invalid value for job_id: %s (type: %s)" % (
                    job_id, type(job_id)))
                break

            # constraint: job identifiers must be unique
            job_node_key = (_NODE_TYPE.JOB, job_id)
            if (job_node_key in self._graph):
                delayed_exception = errors.SpateException(
                    "a job with identifier '%s' already exists" % job_id)
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
                    "job '%s' has no input nor output" % job_id)
                break

            self._graph.add_node(
                job_node_key,
                _template = None,
                _data = None)

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

            job_ids.append(job_id)

            if (job_data is None):
                job_data = {}

            try:
                self.set_job_template(job_id, template)
                self.set_job_data(job_id, **job_data)

            except Exception as e:
                delayed_exception = e
                break

            logger.debug("job '%s' added (inputs: %s; outputs: %s)" % (
                job_id, ' '.join(input_paths), ' '.join(output_paths)))

        if (delayed_exception is None):
            # constraint: any given path is the product of at most one job
            for (node_type, path) in self._graph.nodes():
                if (node_type != _NODE_TYPE.PATH):
                    continue

                path_node_key = (node_type, path)
                if (self._graph.in_degree(path_node_key) < 2):
                    continue

                producing_job_ids = ["'%s'" % job_id for (_, job_id) in \
                    self._graph.predecessors(path_node_key)]

                delayed_exception = errors.SpateException(
                    "path '%s' is created by more than one job: %s" % (
                    path, ', '.join(producing_job_ids)))
                break

        if (delayed_exception is None):
            # constraint: the workflow must be a directed acyclic graph
            if (not networkx.is_directed_acyclic_graph(self._graph)):
                delayed_exception = errors.SpateException(
                    "unable to add job%s %s without creating cycles" % (
                        's' if (len(job_ids) > 1) else '',
                        ', '.join(["'%s'" % job_id for job_id in job_ids])))

        # if any exception was thrown by one of the job addition,
        # we remove all jobs that were added in this transaction
        if (delayed_exception is not None):
            for job_id in job_ids:
                self.remove_job(job_id)

            raise delayed_exception

        return job_ids

    def remove_job (self, job_id):
        """ Remove an existing job from this workflow

            Arguments:
                job_id (str): identifier of the job to remove

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)
        input_paths, output_paths = self.get_job_paths(job_id)

        # remove the job node itself, then
        self._graph.remove_node(job_node_key)
        logger.debug("job '%s' removed" % job_id)

        # remove any input or output path
        # that would be left disconnected
        for path in input_paths + output_paths:
            path_node_key = (_NODE_TYPE.PATH, path)
            if (path_node_key in self._graph) and \
               (self._graph.degree(path_node_key) == 0):
                self._graph.remove_node(path_node_key)
                logger.debug("removed orphan path '%s'" % path)

    def has_job (self, job_id):
        """ Test if a job is part of this workflow

            Arguments:
                job_id (str): job identifier

            Returns:
                boolean: True if a job exists with this identifier,
                    False otherwise
        """
        return (job_id in self)

    def __contains__ (self, job_id):
        if (not utils.is_string(job_id)):
            return False

        return ((_NODE_TYPE.JOB, job_id) in self._graph)

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
                str: job identifier
                str, list of str, list of str: job identifier, list of input
                    paths, and list of output paths (if with_paths = True)
                [str, int]: job identifier and status (if with_status = True)
                [str, int], list of [str, int], list of [str, int]: job
                    identifier and job status, list of input paths and path
                    status, and list of output paths and path status (if
                    with_status = True and with_paths = True)

            Notes:
            [1] The integer returned when using the with_status option are
                taken from the `JOB_STATUS` and `PATH_STATUS` enum sets for
                job and path status, respectively
        """
        # (1) retrieve paths modification time and jobs order
        path_mtime, job_identifiers = {}, []
        for (node_type, node) in networkx.topological_sort(self._graph):
            if (node_type == _NODE_TYPE.PATH):
                path_mtime[node] = utils.path_mtime(node)
            elif (node_type == _NODE_TYPE.JOB):
                job_identifiers.append(node)

        # (2) identify jobs that need to be re-run, either...
        outdated_paths = {}  # paths that will be re-generated by another job
        for job_id in job_identifiers:
            input_paths, output_paths = self.get_job_paths(job_id)

            paths_status = {}
            depends_on_prior_jobs = False

            existing_input_paths = []
            for input_path in input_paths:
                # (a) because one of the input is outdated
                if (input_path in outdated_paths):
                    paths_status[input_path] = PATH_STATUS.OUTDATED
                    depends_on_prior_jobs = True

                elif (path_mtime[input_path] is not None):
                    existing_input_paths.append(input_path)

            existing_output_paths = []
            for output_path in output_paths:
                # (b) because one of the output is missing
                if (path_mtime[output_path] is None):
                    paths_status[output_path] = PATH_STATUS.MISSING
                else:
                    existing_output_paths.append(output_path)

            for (input_path, output_path) in itertools.product(
                existing_input_paths, existing_output_paths):
                # (d) because one of the input is newer than one of the output
                if (path_mtime[input_path] > path_mtime[output_path]):
                    paths_status[output_path] = PATH_STATUS.OUTDATED

            if (len(paths_status) == 0):
                job_status = JOB_STATUS.CURRENT

                # we skip this job if only outdated jobs are requested
                if (outdated_only):
                    continue
            else:
                job_status = JOB_STATUS.OUTDATED
                for output_path in existing_output_paths:
                    outdated_paths[output_path] = True

            # any path not flagged at that point is current or missing
            for path in input_paths + output_paths:
                if (not path in paths_status):
                    paths_status[path] = PATH_STATUS.CURRENT
                elif (path_mtime[path] is None):
                    paths_status[path] = PATH_STATUS.MISSING

            # we skip this job if it depends on another job and
            # the user only wants the non-dependent ancestor jobs
            if (depends_on_prior_jobs) and (not with_descendants):
                continue

            # the user only wants job identifiers
            if (not with_paths) and (not with_status):
                yield job_id

            # the user wants status but not paths
            elif (with_status) and (not with_paths):
                yield (job_id, job_status)

            # the user wants paths but not status
            elif (not with_status) and (with_paths):
                yield (job_id, input_paths, output_paths)

            # the users want both paths and status
            else:
                yield (
                    (job_id, job_status),
                    tuple([(input_path, paths_status[input_path]) \
                        for input_path in input_paths]),
                    tuple([(output_path, paths_status[output_path]) \
                        for output_path in output_paths]))

    def get_job_predecessors (self, job_id):
        """ Return job(s) upstream of a given job, if any

            Arguments:
                job_id (str): job identifier

            Returns:
                list of str: list of job identifiers
        """
        job_node_key = self._ensure_existing_job(job_id)

        input_node_keys = []
        for (_, input_path) in self._graph.predecessors(job_node_key):
            input_node_key = (_NODE_TYPE.PATH, input_path)
            edge = self._graph[input_node_key][job_node_key]
            input_node_keys.append((edge["_order"], input_node_key))

        input_jobs = []
        for (_, input_node_key) in sorted(input_node_keys):
            for (_, job_id) in self._graph.predecessors(input_node_key):
                if (not job_id in input_jobs):
                    input_jobs.append(job_id)

        return input_jobs

    def get_job_successors (self, job_id):
        """ Return job(s) downstream of a given job, if any

            Arguments:
                job_id (str): job identifier

            Returns:
                list of str: list of job identifiers
        """
        job_node_key = self._ensure_existing_job(job_id)

        output_node_keys = []
        for (_, output_path) in self._graph.successors(job_node_key):
            output_node_key = (_NODE_TYPE.PATH, output_path)
            edge = self._graph[job_node_key][output_node_key]
            output_node_keys.append((edge["_order"], output_node_key))

        output_jobs = []
        for (_, output_node_key) in sorted(output_node_keys):
            for (_, job_id) in self._graph.successors(output_node_key):
                if (not job_id in output_jobs):
                    output_jobs.append(job_id)

        return output_jobs

    def get_job_paths (self, job_id):
        """ Return input and output paths associated with a job, if any

            Arguments:
                job_id (str): job identifier

            Returns:
                list of str: list of input paths (or empty list)
                list of str: list of output paths (or empty list)

            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)

        input_paths = []
        for (_, input_path) in self._graph.predecessors(job_node_key):
            edge = self._graph[(_NODE_TYPE.PATH, input_path)][job_node_key]
            input_paths.append((edge["_order"], input_path))

        output_paths = []
        for (_, output_path) in self._graph.successors(job_node_key):
            edge = self._graph[job_node_key][(_NODE_TYPE.PATH, output_path)]
            output_paths.append((edge["_order"], output_path))

        return (
            tuple([input_path for (_, input_path) in sorted(input_paths)]),
            tuple([output_path for (_, output_path) in sorted(output_paths)])
        )

    def get_job_template (self, job_id):
        """ Return the template associated with a job, if any

            Arguments:
                job_id (str): job identifier

            Returns:
                str: template associated with this job

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)
        return self._graph.node[job_node_key]["_template"]

    def set_job_template (self, job_id, template):
        """ Set or update a template associated with a job

            Arguments:
                job_id (str): job identifier
                template (str): job template

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)

        if (template is not None) and (not utils.is_string(template)):
            raise ValueError("invalid type for template (should be str): %s" %
                type(template))

        self._graph.node[job_node_key]["_template"] = template

    def get_job_data (self, job_id):
        """ Return a copy of the data associated with a job, if any

            Arguments:
                job_id (str): identifier of the job

            Returns:
                dict: copy of the data associated with this job

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)
        return self._graph.node[job_node_key]["_data"].copy()

    def set_job_data (self, job_id, **job_data):
        """ Set data associated with a job

            Arguments:
                job_id (str): identifier of the job
                **job_data (dict): data for this job

            Returns:
                nothing

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)
        self._graph.node[job_node_key]["_data"] = job_data

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

        for (job_id_a, input_paths_a, output_paths_a), \
            (job_id_b, input_paths_b, output_paths_b) in \
            zip(list_jobs(self), list_jobs(obj)):
            if (job_id_a != job_id_b):
                return False

            if (sorted(input_paths_a) != sorted(input_paths_b)) or \
               (sorted(output_paths_a) != sorted(output_paths_b)):
                return False

            if (self.get_job_template(job_id_a) != \
                obj.get_job_template(job_id_b)):
                return False

            if (self.get_job_data(job_id_a) != \
                obj.get_job_data(job_id_b)):
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
            for (job_id, input_paths, output_paths) in jobs:
                merged_workflow.add_job(
                    input_paths, output_paths,
                    "%s:%s" % (workflow.name, job_id),
                    workflow.job_template(job_id),
                    **workflow.job_data(job_id))

        logger.debug("merged workflow %s with %s" % (self, obj))
        return merged_workflow

    def __str__ (self):
        return "<spate.workflow name:'%s' njobs:%d npaths:%d>" % (
            self.name, self.number_of_jobs, self.number_of_paths)

def new_workflow (name = None):
    """ Create a new workflow object

        Arguments:
            name (str, optional): name of the workflow

        Returns:
            object: a workflow object
    """
    return _workflow(name)
