# Spate workflow composition toolkit

**Spate** is a lightweight [Python](https://www.python.org/)-based file processing workflow *composition* and *visualization* toolkit. It offers an API to create and export workflows to various popular execution engines, as well as shells and [job schedulers](https://en.wikipedia.org/wiki/Job_scheduler).

The purpose of **Spate** is to offer a simple way to design a *dataflow instance* (i.e., file-based data processing steps, and how they depend on each others) and export it to one or more *execution resources* (i.e., hardware and software environments) where it will run. Add jobs, remove jobs, merge workflows; **Spate** will take care of the details.

**Spate** can export the whole workflow, or only the steps that need to be executed. **Spate** checks input and output files for modification time, and iteratively identify all steps that need to be run or updated. No more worries about re-running a whole time-consuming workflow when you update few files; **Spate** will ensure that only the affected steps are executed.

## Example

This workflow will take two input files, `file_A` and `file_B`, and process them to create an output file `file_C` by running the Unix `cat` command:

```python
import spate

# create a new empty workflow
workflow = spate.new_workflow("my_workflow")

# add a simple job; here a Unix shell
# command to merge two files into one
workflow.add_job(
	inputs = ("file_A", "file_B"),  # two input files
	outputs = "file_C",  # one output file
	content = """
	   cat {{INPUT0}} {{INPUT1}} > {{OUTPUT}}
	   """)

# export this workflow as a shell script; to run
# it, type `./my_workflow.sh` the the shell
spate.to_shell_script(workflow, "my_workflow.sh")

# export this workflow as a SLURM sbatch script; to run it,
# type `sbatch my_workflow.slurm` on a SLURM-enabled cluster
spate.to_slurm(workflow, "my_workflow.slurm")
```

> see `doc/` for additional examples

## Installation

**Spate** is hosted both on [PyPI](https://pypi.python.org/pypi/spate) and [GitHub](https://github.com/ajmazurie/spate), thus offering two options to install it:

- If using **PyPI** (recommended to get the latest release) and the [pip](https://pip.pypa.io/en/stable/) package manager, type `pip install spate`  on the command line. 
- If using **GitHub** (in case you want one of the development version) you can download the archive of one of the [releases](https://github.com/ajmazurie/spate/releases) or [commits](https://github.com/ajmazurie/spate/commits/master) and run `pip install <archive>`.

## Supported execution environments

Environment | Status | Comment
--- | --- | ---
Unix shell scripts | Stable | No parallel job execution; jobs are processed sequencially.
[Make](https://en.wikipedia.org/wiki/Makefile) files | Stable |
[Makeflow](http://ccl.cse.nd.edu/software/makeflow/) scripts | Experimental | *Not fully tested.*
[Drake](https://github.com/Factual/drake) scripts | Experimental | *Not fully tested.*
[SLURM](http://slurm.schedmd.com/) job scheduler | Stable | Produces [sbatch](http://slurm.schedmd.com/sbatch.html) scripts with directed acyclic graph of jobs. Tested with SLURM 14.11.11.
[TORQUE/PBS](https://en.wikipedia.org/wiki/TORQUE) job scheduler  | Experimental | Produces job arrays; does not handle job dependencies. Currently untested.

<!--[HTCondor](https://research.cs.wisc.edu/htcondor/) job scheduler | Experimental | Produces [DAGMan](https://research.cs.wisc.edu/htcondor/dagman/dagman.html) scripts. Tested with HTCondor 8.4.4-->

<!--The following environments are considered for future releases: [Pegasus](https://pegasus.isi.edu/), [Swift](http://swift-lang.org/).-->
