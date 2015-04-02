
import utils

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
    def __init__ (self, name = None):
        if (name is None):
            name = utils.random_string()

        self._data = {}
        self._graph = networkx.DiGraph(name = name)

        logger.debug("created a new workflow with name '%s'" % name)

    def get_name (self):
        """ Return the workflow name
        """
        return self._graph.graph["name"]

    def set_name (self, name):
        """ Set the workflow name
        """
        self._graph.graph["name"] = name
        logger.debug("workflow name set to '%s'" % name)

    name = property(get_name, set_name)

    def _ensure_existing_job (self, job_id):
        job_node_key = (_NODE_TYPE.JOB, job_id)
        if (not job_node_key in self._graph):
            raise ValueError("unknown job '%s'" % job_id)

        return job_node_key

    def add_job (self, inputs = None, outputs = None, name = None, **kwargs):
        """ Add a job to this workflow
        """
        input_paths = utils.ensure_iterable(inputs)
        output_paths = utils.ensure_iterable(outputs)

        # a default job identifier is created if none is provided
        if (name is None):
            job_id = "JOB_%d" % (len(filter(
                lambda (node_type, node): (node_type == _NODE_TYPE.JOB),
                self._graph.nodes_iter())) + 1)
        else:
            job_id = str(name)

        # constraint: a given job can only be declared once
        job_node_key = (_NODE_TYPE.JOB, job_id)
        if (job_node_key in self._graph):
            raise Exception("a job with name '%s' already exists" % job_id)

        # constraint: inputs and outputs must not have duplicates
        utils.ensure_unique(input_paths)
        utils.ensure_unique(output_paths)

        # constraint: a job must have at least one input or output
        if (len(input_paths) == 0) and (len(output_paths) == 0):
            raise Exception("job '%s' has no input nor output" % job_id)

        # constraint: any given path is the product of at most one job
        for output_path in output_paths:
            path_node_key = (_NODE_TYPE.PATH, output_path)
            if (path_node_key in self._graph) and \
               (self._graph.in_degree(path_node_key) > 0):
                _, producing_job_id = self._graph.predecessors(path_node_key)[0]
                raise Exception("path '%s' is already created by job '%s'" % (
                    output_path, producing_job_id))

        self._graph.add_node(
            job_node_key,
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
            raise Exception(
                "unable to add job '%s' without creating cycles" % job_id)

        logger.debug("job '%s' added (inputs: %s; outputs: %s)" % (
            job_id, ' '.join(input_paths), ' '.join(output_paths)))

        return job_id

    def remove_job (self, name):
        """ Remove an existing job from this workflow
        """
        job_node_key = self._ensure_existing_job(name)
        input_paths, output_paths = self.job_inputs_and_outputs(name)

        # remove the job node itself, then
        self._graph.remove_node(job_node_key)

        # remove any input or output path
        # that would be left disconnected
        for path in input_paths + output_paths:
            path_node_key = (_NODE_TYPE.PATH, path)
            if (path_node_key in self._graph) and \
               (self._graph.degree(path_node_key) == 0):
                self._graph.remove_node(path_node_key)

        logger.debug("job '%s' removed" % name)

    @property
    def number_of_jobs (self):
        """ Return the number of jobs in this workflow
        """
        return len(filter(
            lambda (node_type, node): (node_type == _NODE_TYPE.JOB),
            self._graph.nodes_iter()))

    @property
    def number_of_paths (self):
        """ Return the number of paths in this workflow
        """
        return len(filter(
            lambda (node_type, node): (node_type == _NODE_TYPE.PATH),
            self._graph.nodes_iter()))

    def job_data (self, name):
        """ Return data associated with a job, if any
        """
        job_node_key = self._ensure_existing_job(name)
        return self._graph.node[job_node_key]["_data"]

    def job_inputs_and_outputs (self, name, to_absolute = False):
        """ Return input and output paths associated with a job, if any
        """
        job_node_key = self._ensure_existing_job(name)

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

    def list_jobs (self, outdated_only = True, with_descendants = True,
        with_paths = False, with_explanations = False):
        """ List jobs in this workflow, in the order of their execution
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

def new_workflow (name = None):
    return _workflow(name)

def from_json (data):
    """ Create a new workflow object from a JSON object
    """
    try:
        w = _workflow(data["workflow"]["name"])
        for job in data["jobs"]:
            w.add_job(
                job["inputs"],
                job["outputs"],
                job["id"],
                **job["data"])

        return w

    except KeyError as e:
        raise ValueError("invalid JSON document: missing key '%s'" % e.args[0])

def load (filename):
    """ Create a new workflow object from a JSON-formatted file
    """
    if (filename.lower().endswith(".gz")):
        fh = gzip.open(filename, "rb")
    else:
        fh = open(filename, "rU")

    return from_json(json.load(fh))

def to_json (workflow, outdated_only = True):
    """ Export a workflow as a JSON object
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
            ("data", workflow.job_data(job_id)),
        )))

    return data

def save (workflow, filename, outdated_only = True):
    """ Export a workflow as a JSON-formatted file
    """
    if (filename.lower().endswith(".gz")):
        fh = gzip.open(filename, "wb")
    else:
        fh = open(filename, 'w')

    json.dump(
        to_json(workflow, outdated_only),
        fh,
        indent = 4,
        separators = (',', ': '))
