# Spate workflow generation toolkit

**Spate** is a Python-based data processing workflow *composition* and *visualization* toolkit with export functions to various popular workflow execution engines and batch schedulers.

The purpose of **Spate** is to offer a simple way to design a *dataflow instance* (i.e., file-based processing steps, and how they relate to each others) and export it to one ore more *execution resources* (i.e., hardware and software environments) where it will run. Add jobs, remove jobs, merge workflows; **Spate** will take care of the details. Write once, run anywhere.

**Spate** can export your whole workflow, or only the steps that need to be executed. **Spate** checks input and output files for modification time, and iteratively identify all steps that need to be run or updated. No more worries about re-running a whole time-consuming workflow when you update few files; **Spate** will ensure that only the affected steps are executed.

A quick example:

```python
import spate

# create a new empty workflow
workflow = spate.new_workflow("my_workflow")

# add a simple job; here a Unix shell
# command to merge two files into one
workflow.add_job(
	inputs = ("file_A", "file_B"),  # two input files
	outputs = "file_C",  # one output file
	content = "cat {{INPUT0}} {{INPUT1}} > {{OUTPUT}}")

# use the Mustache templating engine
# to build the job's body out of the
# generic template we declared above
spate.set_template_engine(spate.mustache_template_engine)

# export this workflow as a shell script
spate.to_shell_script(workflow, "my_workflow.sh")

# export this workflow as a SLURM sbatch script
spate.to_slurm(workflow, "my_workflow.slurm")
```

At the time of writing **Spate** supports the following export targets:

- *nix **shell** scripts (e.g., BASH or SH)
- *nix **Make** files
- **Makeflow** workflow execution engine scripts
- **Drake** workflow execution engine scripts
- **TORQUE/PBS** batch scheduler job arrays
- **SLURM** batch scheduler sbatch scripts
