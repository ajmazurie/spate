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
