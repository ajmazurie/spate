# (very) Quickstart

**Spate** is a lightweight library to manipulate file-based data processing workflows, decoupled from the software (and hardware) environment that will execute this workflow. Because of this decoupling you can design a workflow without worrying about where to run it, and focus on its logic instead. Since **Spate** is a Python library, you can use complex Python logic to build your workflow the way you want.

With **Spate** you declare your jobs as accepting input *paths* (i.e., files or directories) and/or producing output paths. **Spate** will automatically identify the jobs that need to be executed based on the existence of these paths, and their modification time. For example, a job accepting an input file ``A`` and producing an output file ``B`` will be flagged for execution either if ``B`` is missing, or if ``A`` is more recent than ``B``.

**Spate** can manage job dependencies regardless of the size or complexity of your workflow. If a job flagged for execution produces files that are used by other jobs, these jobs will also be flagged for execution.

Finally, **Spate** knows how to run your workflow on various software platforms, including a plain shell script, existing workflow systems such as [Makeflow](http://ccl.cse.nd.edu/software/makeflow/), [Drake](https://github.com/Factual/drake), or job schedulers such as [TORQUE/PBS](http://www.adaptivecomputing.com/products/open-source/torque/) and [SLURM](http://slurm.schedmd.com/). You only need to call a simple function to export an existing **Spate** workflow to any of these platforms; **Spate** will deal with the messy details for you.

## Installation

**Spate** is a standard, pure Python library which can be downloaded from its dedicated Github [repository]() and installed with `pip`:

``` shell
$ wget <path_to_github_release>
$ pip install <path_to_package>
```

Note that **Spate** will also install some mandatory dependencies automatically. Optional dependencies can also be installed by the user to enable the use of additional functions. To install these dependencies, you can use `pip` again:

```shell
$ pip install -r OPTIONAL_DEPENDENCIES
```

## Usage

### Example 1. Basic abstract workflow

**Spate** is meant to be used by other Python scripts, and once installed can be imported by typing `import spate`. Here is an example script `example_1.py`:

```python
import spate

# create a new workflow
workflow = spate.new_workflow("example-1")

# add some jobs
workflow.add_job("A", ("B", "C"), job_id = "x")  # A-[x]->B,C
workflow.add_job(("A", "C"), "D", job_id = "y")  # A,C-[y]->D

# print some basic information about this workflow
print "number of jobs:", workflow.number_of_jobs
print "number of paths:", workflow.number_of_paths

# print the jobs in this workflow, in the order of their
# execution; note that for the 'colorized' option to work,
# the optional dependency 'Colorama' must be installed
spate.echo(workflow, colorized = True)

# save this workflow for latter use
spate.save(workflow, "example_1.spate.gz")
```

Here we created a simple workflow with two jobs. Note that for convenience and readability the `add_job()` function accepts either single strings or list of strings to declare input and output paths.

Running this script will create the workflow, save it as a file for later use, and display the jobs that will be executed (in the right order) if the workflow was to be run:

```shell
$ python example_1.py
number of jobs: 2
number of paths: 4
< A
x
> B
> C

< A
< C
y
> D

total: 2 outdated jobs (out of 2)
```

Here the `spate.echo()` function generated a simple list of job identifiers preceded by input paths (lines prefixed with `<`) and followed by output paths (lines prefixed with `>`). If this workflow was to be executed, job `x` would be run before job `y`.

### Example 2. Basic concrete workflow

In the previous example our jobs did not have any code attached; as such, our workflow was purely abstract. To attach a piece of code to a job you can use the `template` argument of the `add_job()` function. This argument accepts a string with your code. For example, given the following script `example_2.py`:

```python
import spate

# create a new workflow
workflow = spate.new_workflow("example-2")

# add some jobs, with Unix shell-compatible code
workflow.add_job(
    inputs = "A",
    outputs = ("B", "C"),
    job_id = "x",
    template = """
        grep my_pattern A > B
        grep -v my_pattern A > C
    """)

workflow.add_job(
    inputs = ("A", "C"),
    outputs = "D",
    job_id = "y",
    template = "cat A C > D")

# export this workflow as a shell script (BASH by default)
spate.to_shell_script(workflow, "example_2.sh")
```

Upon execution this script will generate an executable BASH shell script (also other shells can be used as well) that is ready to run:

```shell
$ python example_2.py
$ ls
example_2.py example_2.sh
$ less example_2.sh
#/bin/bash

# x
grep my_pattern A > B
grep -v my_pattern A > C

# y
cat A C > D

$ ./example_2.sh
```

### Example 3. Advanced concrete workflow

You may have noticed that the code for both jobs `x` and `y` in our previous example had the name of the input and output paths hardcoded. This is inconvenient if you want to write generic code (i.e., code that would work regardless of the name of the input and/or output paths). To solve this problem **Spate** allow the use of *template engines*.

A template engine will look for specific tags in your job code and replace them by the content of some variables. By default, all jobs have the following variables accessible to their code:

name | content
--- | ---
`INPUTS` | List of input paths
`INPUTN` | Number of input paths
`INPUT` | First input path
`INPUTn` | Input path in position *n*, starting from zero
`OUTPUTS` | List of output paths
`OUTPUTN` | Number of output paths
`OUTPUT` | First output path
`OUTPUTn` | Output path in position *n*, starting from zero

The tags you will use in your job code are dependent of the template engine you chose. **Spate** comes with two engines: a very simple one using the `string.Template` class (see [here](https://docs.python.org/2/library/string.html#template-strings) for a documentation of its syntax), and a more comprehensive one using the Mustache syntax (see [here](http://mustache.github.io/) for the documentation).

Setting a template engine for your workflow is as simple as calling the `spate.set_template_engine()` function. Here are two examples (one for each template engine), `example_3a.py` and `example_3b.py`:

```python
import spate

# set the template engine to the default
# one, which will use string.Template
spate.set_template_engine(spate.default_engine)

workflow = spate.new_workflow("example-3a")

# declare jobs using a compatible template
workflow.add_job(
    inputs = "A",
    outputs = ("B", "C"),
    job_id = "x",
    template = """
        grep my_pattern $INPUT > $OUTPUT0
        grep -v my_pattern $INPUT > $OUTPUT1
    """)

workflow.add_job(
    inputs = ("A", "C"),
    outputs = "D",
    job_id = "y",
    template = "cat $INPUT0 $INPUT1 > $OUTPUT")

# export this workflow as a shell script
spate.to_shell_script(workflow, "example_3a.sh")
```

```python
import spate

# set the template engine to Mustache (note that the optional
# dependency 'pystache' must be installed for this to work)
spate.set_template_engine(spate.mustache_engine)

workflow = spate.new_workflow("example-3b")

# declare jobs using a compatible template
workflow.add_job(
    inputs = "A",
    outputs = ("B", "C"),
    job_id = "x",
    template = """
        grep my_pattern {{INPUT}} > {{OUTPUT0}}
        grep -v my_pattern {{INPUT}} > {{OUTPUT1}}
    """)

workflow.add_job(
    inputs = ("A", "C"),
    outputs = "D",
    job_id = "y",
    template = "cat {{#INPUTS}}{{.}} {{/INPUTS}}> {{OUTPUT}}")

# export this workflow as a shell script
spate.to_shell_script(workflow, "example_3b.sh")
```

As you will notice we are using an ability from Mustache to loop through a list to write the code associated to job `y`; no need to explicitly mention each input path here.

The two examples produce exactly the same shell script; the only difference is how **Spate** interprets the code template attached to each job.