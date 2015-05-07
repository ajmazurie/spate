
import collections
import json
import logging

from .. import core
from .. import errors
import utils

import enum
import yaml

__all__ = (
    "from_json",
    "from_yaml",
    "load",
    "to_json",
    "to_yaml",
    "save")

logger = logging.getLogger(__name__)

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
                  "inputs": list of str,  # list of input paths (optional)
                  "outputs": list of str,  # list of output paths (optional)
                  "template": str,  # job template (optional)
                  "data": dict,  # data associated with this job (optional)
                },
                ...
              ]
            }
    """
    try:
        w = core.new_workflow(data["workflow"]["name"])

        def jobs():
            for job in data["jobs"]:
                yield (
                    job.get("inputs"),
                    job.get("outputs"),
                    job.get("template"),
                    job["id"],
                    job.get("data", {}))

        w.add_jobs(jobs())
        return w

    except KeyError as e:
        raise ValueError("invalid JSON document: missing key '%s'" % e.args[0])

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
    utils.ensure_workflow(workflow)

    data = {
        "workflow": {
            "name": workflow.name
        },
        "jobs": []
    }

    for job_id in sorted(workflow.list_jobs(outdated_only = outdated_only)):
        job_inputs, job_outputs = workflow.get_job_paths(job_id)
        job_entry = collections.OrderedDict(id = job_id)

        if (len(job_inputs) > 0):
            job_entry["inputs"] = job_inputs

        if (len(job_outputs) > 0):
            job_entry["outputs"] = job_outputs

        job_template = workflow.get_job_template(job_id)
        if (job_template is not None) and (job_template.strip() != ''):
            job_entry["template"] = job_template

        job_data = workflow.get_job_data(job_id)
        if (len(job_data) > 0):
            job_entry["data"] = job_data

        data["jobs"].append(job_entry)

    return data

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

def from_yaml (data):
    """ Create a new workflow object from a YAML document

        Arguments:
            data (str): a YAML document

        Returns:
            object: a workflow object

        [1] The YAML document must comply to the following schema:
            ---
            workflow:
                name: <str>  # name of the workflow
            jobs:
            - id: <str>  # job identifier
              inputs:  # list of input paths (optional)
              - <str>
              outputs:  # list of output paths (optional)
              - <str>
              template: <str>  # job template (optional)
              data:  # job data (optional)
                <key>: <value>
            ...
    """
    try:
        data = yaml.load(data)

    except yaml.YAMLError as e:
        raise errors.SpateException("invalid YAML document: %s" % e)

yaml.add_representer(tuple,
    yaml.representer.SafeRepresenter.represent_list)

yaml.add_representer(collections.OrderedDict,
    yaml.representer.SafeRepresenter.represent_dict)

def to_yaml (workflow, outdated_only = True):
    """ Export a workflow as a YAML document

        Arguments:
            workflow (object): a workflow object
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported

        Returns:
            str: a YAML document

        Notes:
        [1] The YAML document is formatted as shown in the documentation of the
            `from_yaml` method
    """
    return yaml.dump(to_json(workflow, outdated_only))

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

def load (source):
    """ Create a new workflow object from a JSON- or YAML-formatted file

        Arguments:
            source (str or file object): the name of the JSON- or YAML-
                formatted file, or a file object open in reading mode

        Returns:
            object: a workflow object

        Notes:
        [1] If the source is a string with extension '.gz' or '.bz2', the
            corresponding file will be decompressed with the GZip or BZip2
            algorithm, respectively
        [2] The JSON-formatted file must comply to the schema shown in the
            documentation of the `from_json` method
        [3] The YAML-formatted file must comply to the schema shown in the
            documentation of the `from_yaml` method
    """
    source_fh, is_named_source = utils.stream_reader(source)
    raw_data, data = source_fh.read(), None

    try:
        data = json.loads(raw_data)
    except:
        pass

    try:
        data = yaml.load(raw_data)
    except:
        pass

    if (data is None):
        raise errors.SpateException("unknown format for input %s" % source)

    return from_json(data)

class _FILE_FORMAT (enum.Enum):
    JSON = 0
    YAML = 1

def save (workflow, target, outdated_only = True):
    """ Export a workflow as a JSON or YAML-formatted file

        Arguments:
            workflow (object): a workflow object
            target (str or file object): the name of an output file,
                or a file object open in writing mode
            outdated_only (boolean, optional): if set to True, will only export
                jobs that need to be re-run; if False, all jobs are exported

        Returns:
            nothing

        Notes:
        [1] If the target is a string with extension '.gz' or '.bz2', the
            corresponding file will be compressed with the GZip or BZip2
            algorithm, respectively
        [2] The format of the output will be set based on the filename, if
            available; e.g., a '.json' or '.json.gz' extension will produce a
            JSON file, while '.yaml' or '.yaml.gz' will produce a YAML file. If
            no extension is provided JSON is selected as the default format
    """
    data = to_json(workflow, outdated_only)

    target_fh, is_named_target = utils.stream_writer(target)
    target_format = _FILE_FORMAT.JSON

    if (is_named_target):
        if (target.lower().endswith(".yaml") or \
            target.lower().endswith(".yaml.gz")):
            target_format = _FILE_FORMAT.YAML

    if (target_format == _FILE_FORMAT.JSON):
        json.dump(data, target_fh,
            indent = 4,
            separators = (',', ': '))

    elif (target_format == _FILE_FORMAT.YAML):
        yaml.dump(data, stream = target_fh,
            explicit_start = True,
            default_flow_style = False)
