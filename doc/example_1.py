import spate

# create a new workflow
workflow = spate.new_workflow("example-1")

# add some jobs
workflow.add_job("A", ("B", "C"), name = "x")  # A-[x]->B,C
workflow.add_job(("A", "C"), "D", name = "y")  # A,C-[y]->D

# print some basic information about this workflow
print "number of jobs:", workflow.number_of_jobs
print "number of paths:", workflow.number_of_paths

# print the jobs in this workflow,
# in the order of their execution
spate.echo(workflow, colorized = True)

# create a diagram of this workflow;
# note that this requires the optional
# 'pygraphviz' package to be installed
spate.draw(workflow, "example_1.png")

# save this workflow for later use
spate.save(workflow, "example_1.spate.gz")
