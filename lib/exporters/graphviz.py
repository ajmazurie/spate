
from .. import core
from .. import utils

__all__ = (
    "draw",
    "to_graphviz")

def to_graphviz (workflow, filename = None):
    """ Export a workflow as a Graphviz object or Graphviz-formatted file

        Arguments:
            workflow (object): workflow object
            filename (string, optional): name of the output file

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

    node_id = lambda (node_type, node): "%s:%s" % (node_type.name, node)

    for node_key in workflow._graph.nodes_iter():
        node_type, node = node_key
        g.add_node(
            node_id(node_key),
            label = node,
            _node_type = node_type.name)

    for (source_node_key, target_node_key) in workflow._graph.edges_iter():
        g.add_edge(
            node_id(source_node_key),
            node_id(target_node_key))

    if (filename is not None):
        g.write(filename)
    else:
        return g

_GRAPHVIZ_PATH_NODE_FGCOLOR = {
    None:                             (190, 214,  47),
    core.EXPLANATION.MISSING_INPUT:   (217,  27,  92),
    core.EXPLANATION.MISSING_OUTPUT:  (217,  27,  92),
    core.EXPLANATION.POSTDATE_OUTPUT: (252, 176,  64),
    core.EXPLANATION.WILL_UPDATE:     (  0, 174, 239),
}

def draw (workflow, filename, outdated_only = True, decorated = True,
    prog = "dot"):
    """ Export a workflow as a picture, in any format supported by Graphviz

        The jobs are represented in this picture as nodes in a directed graphs,
        showing which files each job produces and consumes and how these files
        are then reused by other jobs.

        Arguments:
            workflow (object): workflow object
            filename (string): name of the output file, with extension
            outdated_only (boolean, optional): if set to True, will only
                display outdated jobs, rather than all jobs by default
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
    g = to_graphviz(workflow)

    # set some generic graphical properties for the graph, nodes and edges
    g.graph_attr["rankdir"] = "LR"
    #g.graph_attr["nodesep"] = 2.0
    g.graph_attr["overlap"] = "scale"
    g.node_attr["shape"] = "box"
    g.node_attr["style"] = "rounded,filled"
    g.node_attr["fontname"] = "Monospace"

    # distinguish between job and path nodes
    for node in g.nodes_iter():
        if (node.attr["_node_type"] == core._NODE_TYPE.JOB.name):
            node.attr["shape"] = "circle"
            node.attr["fontsize"] = 20
            node.attr["fontname"] = "Helvetica"

        elif (node.attr["_node_type"] == core._NODE_TYPE.PATH.name):
            pass

    jobs = workflow.list_jobs(
        outdated_only = outdated_only,
        with_explanations = True)

    for (job_id, explanations) in jobs:
        job_node = g.get_node("%s:%s" % (core._NODE_TYPE.JOB.name, job_id))
        for (path, reason_code) in explanations.iteritems():
            path_node = g.get_node("%s:%s" % (core._NODE_TYPE.PATH.name, path))
            path_node.attr["fillcolor"] = \
                "#%02X%02X%02X" % _GRAPHVIZ_PATH_NODE_FGCOLOR[reason_code]

    # jobs_to_draw = {}
    # for (job_id, explanations) in jobs:
    #     jobs_to_draw[job_id] = True

    # for node in g.nodes_iter():
    #     if (node.attr["_node_type"] == core._NODE_TYPE.JOB.name) and \
    #        (node.label not in jobs_to_draw):
    #        g.

    # if (decorated):
    #     for () in :

    g.draw(filename, prog = prog)
