# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from constructs import Construct
from aws_cdk import (
    Stack,
    Aspects,
    IAspect,
    RemovalPolicy,
    IResource,
    aws_ec2 as ec2,
)

from .agent_service import OpenTelemetryAgentECS
from .data_pipeline import DataPipeline

from constructs import IConstruct
from jsii import implements
from lib.suppressions import add_suppressions


@implements(IAspect)
class RemovalPolicyAspect:
    def visit(self, node: IConstruct) -> None:
        if hasattr(node, "apply_removal_policy"):
            try:
                node.apply_removal_policy(RemovalPolicy.DESTROY)
            except Exception:  # nosec B110
                pass

class OtelSolutionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Creating a VPC
        vpc = ec2.Vpc(
            self, "VPC",
            max_azs=2
        )

        # Instantiating the OpenTelemetryCollectorService Agent
        agent = OpenTelemetryAgentECS(
            self, "OtelAgentService", vpc=vpc,
            enableCloudMap=True
        )

        # Instantiating the Data Pipeline Service
        data_pipeline = DataPipeline(self, "DataPipeline",
                                     vpc=vpc, agent=agent)

        # Apply RemovalPolicy.DESTROY to all resources
        Aspects.of(self).add(RemovalPolicyAspect())

        # Adding Suppressions for CDK NAG
        add_suppressions(self)
