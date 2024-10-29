# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from constructs import Construct
from aws_cdk import (
    Duration,
    Aws,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_ssm as ssm,
    aws_servicediscovery as cloudmap
)
import yaml


def read_yaml_file(file_path):
    with open(file_path, 'r') as file:
        yaml_content = yaml.safe_load(file)
        stringified_yaml = yaml.dump(yaml_content)
    return stringified_yaml


class OpenTelemetryAgentECS(Construct):

    def __init__(self, scope: Construct, id: str,
                 vpc: ec2.IVpc,
                 enableCloudMap: bool,
                 **kwargs):

        super().__init__(scope, id, **kwargs)

        self._otel_config = ssm.StringParameter(
            self, "OtelAgentConfiguration",
            data_type=ssm.ParameterDataType.TEXT,
            description="This parameter hold the configuration for the OpenTelemetry Agent",
            parameter_name="OtelAgentConfiguration",
            string_value=read_yaml_file(
                "config/otel-agent-config.yaml").replace('AWS_REGION', Aws.REGION)
        )

        self._cluster = ecs.Cluster(self, "Cluster",
                                    cluster_name="otel-agent-cluster",
                                    vpc=vpc,
                                    default_cloud_map_namespace=ecs.CloudMapNamespaceOptions(
                                        name='agent',
                                        vpc=vpc,
                                        type=cloudmap.NamespaceType.DNS_PRIVATE) if enableCloudMap else None)

        self._task_role = iam.Role(
            self, "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSXrayWriteOnlyAccess")
            ]
        )

        self._execution_role = iam.Role(
            self, "ExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )

        self._task_definition = ecs.FargateTaskDefinition(
            self, "TaskDefinition",
            family="otel-agent-task-definition",
            cpu=256,
            memory_limit_mib=512,
            task_role=self._task_role,
            execution_role=self._execution_role,
        )

        self._task_definition.add_container(
            "otel-agent",
            image=ecs.ContainerImage.from_registry(
                "amazon/aws-otel-collector"),
            port_mappings=[ecs.PortMapping(container_port=4317),
                           ecs.PortMapping(container_port=55680),
                           ecs.PortMapping(container_port=8889)
                           ],
            logging=ecs.LogDriver.aws_logs(stream_prefix="otel-agent"),
            secrets={
                "AOT_CONFIG_CONTENT": ecs.Secret.from_ssm_parameter(
                    parameter=self._otel_config
                )
            }
        )

        self._service = ecs.FargateService(
            self, "OtelAgentService",
            assign_public_ip=False,
            min_healthy_percent=100,
            max_healthy_percent=200,
            cluster=self._cluster,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            task_definition=self._task_definition,
            desired_count=1,
            cloud_map_options=ecs.CloudMapOptions(
                cloud_map_namespace=self._cluster.default_cloud_map_namespace,
                name="otel",
                dns_record_type=cloudmap.DnsRecordType.A,
                dns_ttl=Duration.seconds(15)
            ) if enableCloudMap else None
        )

        self._service.connections.allow_from(
            other=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            port_range=ec2.Port.tcp(4318),
            description="Allow traffic from VPC for collector"
        )
