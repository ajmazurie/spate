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
