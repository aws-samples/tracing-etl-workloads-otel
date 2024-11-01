# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as cdk

from lib.otel_solution_stack import OtelSolutionStack
from cdk_nag import AwsSolutionsChecks, NagReportLogger, NagReportFormat
import os

app = cdk.App()

# Create an instance of the OtelSolutionStack, passing the app instance and stack name
OtelSolutionStack(app, "OtelSolutionStack", env=cdk.Environment(region=os.environ.get("AWS_REGION", "us-east-2")))

# Adding CDK Nag Checks
# This line adds the AwsSolutionsChecks aspect to the app
cdk.Aspects.of(app).add(AwsSolutionsChecks(report_formats=[NagReportFormat.CSV]))

# Synthesize the CloudFormation template for the stack
app.synth()