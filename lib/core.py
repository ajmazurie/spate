# Implementation of a file-based data processing workflow

import utils
import errors

import json
import enum
import collections
import itertools
import logging
import gzip

import networkx

__all__ = (
    "new_workflow",
    "from_json",
    "load",
    "to_json",
    "save",
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
    def __init__ (self, workflow_name = None):
        if (workflow_name is None):
            workflow_name = utils.random_string()

        self._graph = networkx.DiGraph(name = workflow_name)
        logger.debug("created a new workflow with name '%s'" % workflow_name)

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

    def set_name (self, workflow_name):
        """ Set the workflow name

            Arguments:
                workflow_name (str): new name for the workflow

            Returns:
                nothing

            Notes:
            [1] The name of a given workflow object can also be modified
                by using the dedicated setter function 'name'
        """
        self._graph.graph["name"] = workflow_name
        logger.debug("workflow name set to '%s'" % workflow_name)

    name = property(get_name, set_name)

    def _ensure_existing_job (self, job_id):
        job_node_key = (_NODE_TYPE.JOB, job_id)
        if (not job_node_key in self._graph):
            raise errors.SpateException("unknown job '%s'" % job_id)

        return job_node_key

    def add_job (self, inputs = None, outputs = None, template = None,
        job_id = None, **kwargs):
        """ Add a job to this workflow

            Arguments:
                inputs (list of str, optional): list of paths (either files
                    or directories) this job accepts as input, if any
                outputs (list of str, optional): list of paths (either files
                    or directories) this job accepts as output, if any
                template (str, optional): template for the job code
                job_id (str, optional): unique identifier for this job
                kwargs (dict, optional): additional variables for this job;
                    these variables will be accessible to the job template

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
            [6] The job template will always receive the variables INPUTN and
                OUTPUTN, which contains the number of input and output paths,
                respectively, and the paths themselves as variables INPUTx and
                OUTPUTy with x being betwen 0 and INPUTN-1 and y being between 0
                and OUTPUTN-1, respectively.
        """
        input_paths = utils.ensure_iterable(inputs)
        output_paths = utils.ensure_iterable(outputs)

        # a default job identifier is created if none is provided
        if (job_id is None):
            job_id = "JOB_%d" % (len(filter(
                lambda (node_type, node): (node_type == _NODE_TYPE.JOB),
                self._graph.nodes_iter())) + 1)
        elif (not utils.is_string(job_id)):
            raise ValueError("invalid value for job_id: %s (type: %s)" % (
                job_id, type(job_id)))

        # constraint: a given job can only be declared once
        job_node_key = (_NODE_TYPE.JOB, job_id)
        if (job_node_key in self._graph):
            raise errors.SpateException(
                "a job with identifier '%s' already exists" % job_id)

        # constraint: inputs and outputs must not have duplicates
        utils.ensure_unique(input_paths)
        utils.ensure_unique(output_paths)

        # constraint: a job must have at least one input or output
        if (len(input_paths) == 0) and (len(output_paths) == 0):
            raise errors.SpateException("job '%s' has no input nor output" % job_id)

        # constraint: any given path is the product of at most one job
        for output_path in output_paths:
            path_node_key = (_NODE_TYPE.PATH, output_path)
            if (path_node_key in self._graph) and \
               (self._graph.in_degree(path_node_key) > 0):
                _, producing_job_id = self._graph.predecessors(path_node_key)[0]
                raise errors.SpateException(
                    "path '%s' is already created by job '%s'" % (
                    output_path, producing_job_id))

        self._graph.add_node(
            job_node_key,
            template = template,
            _data = kwargs)

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

        # constraint: the workflow must be a directed acyclic graph
        if (not networkx.is_directed_acyclic_graph(self._graph)):
            self.remove_job(job_id)
            raise errors.SpateException(
                "unable to add job '%s' without creating cycles" % job_id)

        logger.debug("job '%s' added (inputs: %s; outputs: %s)" % (
            job_id, ' '.join(input_paths), ' '.join(output_paths)))

        return job_id

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
        input_paths, output_paths = self.job_inputs_and_outputs(job_id)

        # remove the job node itself, then
        self._graph.remove_node(job_node_key)

        # remove any input or output path
        # that would be left disconnected
        for path in input_paths + output_paths:
            path_node_key = (_NODE_TYPE.PATH, path)
            if (path_node_key in self._graph) and \
               (self._graph.degree(path_node_key) == 0):
                self._graph.remove_node(path_node_key)

        logger.debug("job '%s' removed" % job_id)

    def has_job (self, job_id):
        """ Test if a job is part of this workflow

            Arguments:
                job_id (str): identifier of the job

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
            input_paths, output_paths = self.job_inputs_and_outputs(job_id)

            paths_status = {}
            depends_on_prior_jobs = False

            current_input_paths = []
            for input_path in input_paths:
                # (a) because one of the input is outdated
                if (input_path in outdated_paths):
                    paths_status[input_path] = PATH_STATUS.OUTDATED
                    depends_on_prior_jobs = True

                # (b) because one of the input is missing
                elif (path_mtime[input_path] is None):
                    paths_status[input_path] = PATH_STATUS.MISSING

                else:
                    current_input_paths.append(input_path)

            current_output_paths = []
            for output_path in output_paths:
                # (c) because one of the output is missing
                if (path_mtime[output_path] is None):
                    paths_status[output_path] = PATH_STATUS.OUTDATED

                else:
                    current_output_paths.append(output_path)

            for (input_path, output_path) in itertools.product(
                current_input_paths, current_output_paths):
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

                # if a job is outdated, all its outputs also are
                for output_path in output_paths:
                    paths_status[output_path] = PATH_STATUS.OUTDATED
                    outdated_paths[output_path] = True

            # any input or output path not flagged at that point is current
            for path in input_paths + output_paths:
                if (not path in paths_status):
                    paths_status[path] = PATH_STATUS.CURRENT

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

    def job_inputs_and_outputs (self, job_id):
        """ Return input and output paths associated with a job, if any

            Arguments:
                job_id (str): identifier of the job

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

    def job_template (self, job_id):
        """ Return the template associated with a job, if any

            Arguments:
                job_id (str): identifier of the job

            Returns:
                str: template associated with this job

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)
        return self._graph.node[job_node_key]["template"]

    def job_data (self, job_id):
        """ Return data associated with a job, if any

            Arguments:
                job_id (str): identifier of the job

            Returns:
                dict: data associated with this job

            Notes:
            [1] A SpateException will be raised if the job doesn't exist
        """
        job_node_key = self._ensure_existing_job(job_id)
        return self._graph.node[job_node_key]["_data"]

    def __add__ (self, obj):
        if (not isinstance(obj, self.__class__)):
            raise ValueError("cannot add '%s' to %s" % (obj, self))

        merged_workflow = Workflow("%s+%s" % (self.name, obj.name))

        for workflow in (self, obj):
            jobs = workflow.list_jobs(outdated_only = False, with_paths = True)
            for (job_id, input_paths, output_paths) in jobs:
                merged_workflow.add_job(
                    input_paths, output_paths,
                    "%s:%s" % (workflow.name, job_id),
                    **workflow.job_data(job_id))

        logger.debug("merged workflow %s with %s" % (self, obj))
        return merged_workflow

    def __str__ (self):
        return "<spate.workflow name:'%s' njobs:%d npaths:%d>" % (
            self.name, self.number_of_jobs, self.number_of_paths)

def new_workflow (workflow_name = None):
    """ Create a new workflow object

        Arguments:
            workflow_name (str, optional): name of the workflow

        Returns:
            object: a workflow object
    """
    return _workflow(workflow_name)

def from_json (data):
    """ Create a new workflow object from a JSON object

        Arguments:
            data (dict): a JSON document

        Returns:
            object: a workflow object

        Notes:
        [1] The JSON document must comply to the following schema:
            {
                "workflow": str  # name of the workflow
                "jobs": [  # list of jobs
                    {
                        "id": str,  # job name
                        "inputs": list of str,  # list of input paths
                        "outputs": list of str,  # list of output paths
                        "template": str,  # job template (optional)
                        "data": dict,  # data associated with this job (optional)
                    },
                    ...
                ]
            }
    """
    try:
        w = _workflow(data["workflow"]["name"])
        for job in data["jobs"]:
            w.add_job(
                job["inputs"],
                job["outputs"],
                job.get("template"),
                job["id"],
                **job.get("data", {}))

        return w

    except KeyError as e:
        raise ValueError("invalid JSON document: missing key '%s'" % e.args[0])

def load (input_file):
    """ Create a new workflow object from a JSON-formatted file

        Arguments:
            input_file (str or file object): the name of the JSON-formatted
                file, or a file object open in reading mode

        Returns:
            object: a workflow object

        Notes:
        [1] The JSON-formatted file must comply to the schema shown in the
            documentation of the `from_json` method
    """
    if (utils.is_string(input_file)):
        if (input_file.lower().endswith(".gz")):
            fh = gzip.open(input_file, "rb")
        else:
            fh = open(input_file, "rU")
    else:
        fh = input_file

    return from_json(json.load(fh))

def to_json (workflow, outdated_only = True):
    """ Export a workflow as a JSON document

        Arguments:
            workflow (object): a workflow object
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported

        Returns:
            dict: a JSON document

        Notes:
        [1] The JSON document is formatted as shown in the documentation of the
            `from_json` method
    """
    if (not isinstance(workflow, _workflow)):
        raise ValueError("invalid value type for workflow")

    data = {
        "workflow": {
            "name": workflow.name
        },
        "jobs": []
    }

    for job_id in sorted(workflow.list_jobs(outdated_only = outdated_only)):
        job_inputs, job_outputs = workflow.job_inputs_and_outputs(job_id)
        data["jobs"].append(collections.OrderedDict((
            ("id", job_id),
            ("inputs", job_inputs),
            ("outputs", job_outputs),
            ("template", workflow.job_template(job_id)),
            ("data", workflow.job_data(job_id)),
        )))

    return data

def save (workflow, output_file, outdated_only = True):
    """ Export a workflow as a JSON-formatted file

        Arguments:
            workflow (object): a workflow object
            output_file (str or file object): the name of a JSON-formatted
                output file, or a file object open in writing mode
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported

        Returns:
            nothing

        Notes:
        [1] The JSON file is formatted as shown in the documentation of the
            `from_json` method
    """
    if (utils.is_string(output_file)):
        if (output_file.lower().endswith(".gz")):
            fh = gzip.open(output_file, "wb")
        else:
            fh = open(output_file, 'w')
    else:
        fh = output_file

    json.dump(
        to_json(workflow, outdated_only),
        fh,
        indent = 4,
        separators = (',', ': '))
