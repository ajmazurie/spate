
from .. import core
from .. import utils

__all__ = (
    "draw",
    "to_graphviz")

def to_graphviz (workflow, filename = None, outdated_only = True):
    """ Export a workflow as a Graphviz object or Graphviz-formatted file

        Arguments:
            workflow (object): workflow object
            filename (string, optional): name of the output file
            outdated_only (boolean, optional): if set to True, will only
                export outdated jobs instead of all jobs by default

        Returns:
            nothing if a filename is provided, or a Pygraphviz object if not

        Note:
        [1] For this function to work, the following must be installed:
            - the Pygraphviz Python library; see http://pygraphviz.github.io/
            - the Graphviz software package; see http://www.graphviz.org/
    """
    if (not isinstance(workflow, core._workflow)):
        raise ValueError("invalid value type for workflow")

    pygraphviz = utils.ensure_module("pygraphviz")

    g = pygraphviz.AGraph(name = workflow.name,
        directed = True, strict = True)

    node_id = lambda node_type, node: "%s:%s" % (node_type.name, node)

    jobs = workflow.list_jobs(
        outdated_only = outdated_only,
        with_status = True,
        with_paths = True)

    for ((job_id, job_status), input_paths_status, output_paths_status) in jobs:
        job_node_key = node_id(core._NODE_TYPE.JOB, job_id)
        g.add_node(job_node_key,
            label = job_id,
            _type = core._NODE_TYPE.JOB.name,
            _status = job_status.name)

        all_paths_status = (
            (True, input_paths_status),
            (False, output_paths_status))

        for (is_input, paths_status) in all_paths_status:
            for (path, path_status) in paths_status:
                path_node_key = node_id(core._NODE_TYPE.PATH, path)

                if (not g.has_node(path_node_key)):
                    g.add_node(path_node_key,
                        label = path,
                        _type = core._NODE_TYPE.PATH.name,
                        _status = path_status.name)

                if (is_input):
                    g.add_edge(path_node_key, job_node_key)
                else:
                    g.add_edge(job_node_key, path_node_key)

    if (filename is not None):
        g.write(filename)
    else:
        return g

_GRAPHVIZ_JOB_NODE_FGCOLOR = {
    core.JOB_STATUS.CURRENT:   (  0, 255,   0),  # bright green
    core.JOB_STATUS.OUTDATED:  (255, 128,   0),  # bright orange
}

_GRAPHVIZ_PATH_NODE_FGCOLOR = {
    core.PATH_STATUS.CURRENT:  (229, 255, 204),  # pale green
    core.PATH_STATUS.MISSING:  (255, 153, 153),  # pale red
    core.PATH_STATUS.OUTDATED: (255, 204, 153),  # pale orange
}

def draw (workflow, filename, outdated_only = True, decorated = True,
    prog = "dot"):
    """ Export a workflow as a picture, in any format supported by Graphviz

        The jobs are represented in this picture as nodes in a directed graphs,
        showing which files each job produces and consumes and how these files
        are then reused by other jobs.

        Arguments:
            workflow (object): workflow object, or pygraphviz AGraph object
            filename (string): name of the output file, with extension
            outdated_only (boolean, optional): if set to True, will only
                display outdated jobs, rather than all jobs by default;
                ignored if workflow is a pygraphviz AGraph object
            decorated (boolean, optional): if set to True, will decorate
                the jobs with colors to show their status
            prog (string, optional): program to use within Graphviz to
                lay out and draw the network of jobs

        Returns:
            nothing

        Notes:
        [1] For this function to work, the following must be installed:
            - the Pygraphviz Python library; see http://pygraphviz.github.io/
            - the Graphviz software package; see http://www.graphviz.org/
        [2] A list of available output formats can be retrieved by running
            `dot "-T?"`; see http://www.graphviz.org/doc/info/output.html
    """
    pygraphviz = utils.ensure_module("pygraphviz")

    if (isinstance(workflow, pygraphviz.AGraph)):
        g = workflow
    else:
        g = to_graphviz(workflow, outdated_only = outdated_only)

    g.graph_attr["rankdir"] = "LR"
    #g.graph_attr["nodesep"] = 2.0
    g.graph_attr["overlap"] = "scale"
    g.node_attr["shape"] = "box"
    g.node_attr["style"] = "rounded,filled"
    g.node_attr["fontname"] = "Monospace"

    for node in g.nodes_iter():
        try:
            if (node.attr["_type"] == core._NODE_TYPE.JOB.name):
                node.attr["shape"] = "circle"
                node.attr["fontsize"] = 20
                node.attr["fontname"] = "Helvetica"

                if (decorated):
                    job_status = core.JOB_STATUS[node.attr["_status"]]
                    node.attr["fillcolor"] = \
                        "#%02X%02X%02X" % _GRAPHVIZ_JOB_NODE_FGCOLOR[job_status]

            elif (node.attr["_type"] == core._NODE_TYPE.PATH.name):
                if (decorated):
                    path_status = core.PATH_STATUS[node.attr["_status"]]
                    node.attr["fillcolor"] = \
                        "#%02X%02X%02X" % _GRAPHVIZ_PATH_NODE_FGCOLOR[path_status]

        except KeyError as e:
            raise Exception(
                "invalid pygraphviz object: missing key '%s' for node %s" % (
                    e.args[0], node))

    g.draw(filename, prog = prog)
