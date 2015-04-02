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
