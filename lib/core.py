# Implementation of a file-based data processing workflow

import utils
import errors

import json
import enum
import collections
import logging
import gzip

import networkx

__all__ = (
    "new_workflow",
    "from_json",
    "load",
    "to_json",
    "save",
    "EXPLANATION")

logger = logging.getLogger(__name__)

class EXPLANATION (enum.Enum):
    """ Explanation provided to justify why a job need to be executed
    """
    MISSING_INPUT = 0  # the input path is missing
    MISSING_OUTPUT = 1  # the output path is missing
    POSTDATE_OUTPUT = 3  # the input path postdate one of the output paths
    WILL_UPDATE = 4  # the input path will be updated by another job

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
        with_paths = False, with_explanations = False):
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
                with_paths (boolean, optional): if set to True, will return the
                    input and output paths for each job along with their identifier
                with_explanations (boolean, optional): if set to True, will
                    return a code explaining why a job need to be re-run
                    along with the job identifier

            Yields: either
                str: job identifier
                str, list of str, list of str: job identifier and its input and
                    output paths (if with_paths = True)
                str, list of [str, int]: job identifier and list of explanations
                    as to why this job need to be re-run, or None if the job
                    doesn't need to be rerun (if with_explanations = True)
                str, list of str, list of str, list of [str, int]: job identifier,
                    input and output paths, and explanations (if with_paths
                    and with_explanations = True)

            Notes:
            [1] The integer returned when using the with_explanations option
                are taken from the `EXPLANATION` enum set
        """
        # (1) retrieve paths modification time and jobs order
        mtime, jobs = {}, []
        for (node_type, node) in networkx.topological_sort(self._graph):
            if (node_type == _NODE_TYPE.PATH):
                mtime[node] = utils.path_mtime(node)
            elif (node_type == _NODE_TYPE.JOB):
                jobs.append(node)

        # (2) identify jobs that need to be re-run
        will_update_path = {}  # paths that will be re-generated by another job
        for job_id in jobs:
            input_paths, output_paths = self.job_inputs_and_outputs(job_id)

            reasons_for_execution = {}
            depends_on_prior_jobs = False

            # check if this job need to be re-run, either because
            # (a) one of the input is missing or will be re-created by another job
            for input_path in input_paths:
                if (input_path in will_update_path):
                    reasons_for_execution[input_path] = EXPLANATION.WILL_UPDATE
                    depends_on_prior_jobs = True
                elif (mtime[input_path] is None):
                    reasons_for_execution[input_path] = EXPLANATION.MISSING_INPUT

            # (b) one of the output is missing, or
            for output_path in output_paths:
                if (mtime[output_path] is None):
                    reasons_for_execution[output_path] = EXPLANATION.MISSING_OUTPUT

            # (c) one of the input is newer than one of the output
            for input_path in input_paths:
                if (mtime[input_path] is None) or \
                   (input_path in will_update_path):
                    continue

                postdate_output = False
                for output_path in output_paths:
                    if (mtime[output_path] is None):
                        continue

                    if (mtime[input_path] > mtime[output_path]):
                        postdate_output = True
                        break

                if (postdate_output):
                    reasons_for_execution[input_path] = EXPLANATION.POSTDATE_OUTPUT

            will_update_job = (len(reasons_for_execution) > 0)

            # if this job is planned for execution,
            if (will_update_job):
                # we declare all its outputs as being poised for update
                for output_path in output_paths:
                    will_update_path[output_path] = True
                    reasons_for_execution[output_path] = EXPLANATION.WILL_UPDATE

            # if not we skip it if the user only wants outdated jobs
            elif (outdated_only):
                continue

            # we also skip this job if it depends on another job and
            # the user only wants the non-dependent ancestor jobs
            if (depends_on_prior_jobs) and (not with_descendants):
                continue

            if (not with_paths) and (not with_explanations):
                yield job_id
            else:
                rset = [job_id]

                if (with_paths):
                    rset.append(input_paths)
                    rset.append(output_paths)

                if (with_explanations):
                    if (len(reasons_for_execution) == 0):
                        reasons_for_execution = None

                    rset.append(reasons_for_execution)

                yield tuple(rset)

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
