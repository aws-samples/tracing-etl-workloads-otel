import aws_cdk as cdk

from lib.otel_solution_stack import OtelSolutionStack

app = cdk.App()

OtelSolutionStack(app, "OtelSolutionStack", env=cdk.Environment(region="us-east-2"))

app.synth()
