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
