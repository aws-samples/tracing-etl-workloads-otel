from constructs import Construct

from lib.agent_service import OpenTelemetryAgentECS

from aws_cdk import (
    Aws,
    Fn,
    Token,
    RemovalPolicy,
    Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_s3_assets as assets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as eventsources,
    aws_glue as glue
)

from aws_cdk.aws_ec2 import IVpc


class DataPipeline(Construct):
    def __init__(self, scope: Construct, id: str, vpc: IVpc, agent: OpenTelemetryAgentECS, **kwargs):

        super().__init__(scope, id, **kwargs)

        # Creating a bucket to store all ingested files
        # https://www.kaggle.com/datasets/arianazmoudeh/airbnbopendata

        self._ingest_bucket = s3.Bucket(self, "IngestionBucket",
                                        bucket_name=f"otelsolution-ingest-bucket-{
                                            Aws.ACCOUNT_ID}-{Aws.REGION}",
                                        auto_delete_objects=True,
                                        removal_policy=RemovalPolicy.DESTROY,
                                        block_public_access=s3.BlockPublicAccess.BLOCK_ALL
                                        )

        # Glue Jobs for Processing Data from the AirBnb Dataset

        self.agent_security_group = agent._service.connections.security_groups[
            0].security_group_id

        if agent._service.cloud_map_service:
            self.agent_otlp_endpoint = Fn.join(".", [
                agent._service.cloud_map_service.service_name,
                agent._service.cloud_map_service.namespace.namespace_name,
            ])
        else:
            raise ValueError(
                "Cloud Map service is not enabled for the OpenTelemetry Agent")

        # Glue Job Security Group
        self.glue_security_group = ec2.SecurityGroup(self, "SecurityGroupForGlue",
                                                     vpc=vpc,
                                                     description="Security group for Glue jobs",
                                                     allow_all_outbound=True
                                                     )
        # Add an ingress rule to Glue Security Group that allows all traffic from Agent Security Group
        self.glue_security_group.add_ingress_rule(ec2.Peer.security_group_id(
            self.agent_security_group), ec2.Port.all_traffic())

        # Add an ingress rule to Glue Security Group that allows all traffic from itself
        self.glue_security_group.add_ingress_rule(ec2.Peer.security_group_id(
            self.glue_security_group.security_group_id), ec2.Port.all_traffic())

        # Glue Connection for Connecting to Agent Service VPC
        self.network_connection = glue.CfnConnection(
            self,
            "NetworkConnection",
            catalog_id=Aws.ACCOUNT_ID,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="OTelAgentServiceConnection",
                connection_type="NETWORK",
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=vpc.private_subnets[0].subnet_id,
                    security_group_id_list=[
                        self.glue_security_group.security_group_id],
                    availability_zone=vpc.private_subnets[0].availability_zone
                )
            )
        )

        # Glue Service Role
        self.glue_role = iam.Role(
            self,
            "GlueServiceRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies={
                "AccessToGlueConnection": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["glue:GetConnection"],
                            resources=[
                                Fn.join(
                                    ":", [
                                        "arn",
                                        Aws.PARTITION,
                                        "glue",
                                        Aws.REGION,
                                        Aws.ACCOUNT_ID,
                                        "connection/*"
                                    ]
                                ),
                                Fn.join(
                                    ":", [
                                        "arn",
                                        Aws.PARTITION,
                                        "glue",
                                        Aws.REGION,
                                        Aws.ACCOUNT_ID,
                                        "catalog*"
                                    ]
                                )
                            ]
                        ),
                        iam.PolicyStatement(
                            actions=["ec2:DescribeSubnets",
                                     "ec2:DescribeSecurityGroups"],
                            resources=['*']
                        ),
                        iam.PolicyStatement(
                            actions=["kms:Decrypt",
                                     "kms:GenerateDataKey"],
                            resources=["*"]
                        )
                    ]
                )
            },
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSXrayReadOnlyAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole")
            ],
        )

        # Upload X-Ray Corellation Script to S3
        self.xray_correlation_script = assets.Asset(
            self, "XRayCorrelationScript", path="source/x_ray_helper/xray_helper.py")

        # Providing Glue Service Role Access to Data Bucket

        self._ingest_bucket.grant_read_write(self.glue_role)

        # This job is cleaning and transforming the Airbnb dataset. The job then reads the Airbnb dataset from an S3 bucket and defines several functions to perform various data cleaning and transformation tasks, such as dropping unwanted columns, converting column types, fixing skewness, correcting review dates, standardizing column names, removing duplicates, and filling in null values. The script also applies filters to remove listings with invalid 'minimum_nights' values, rounds the 'number_of_reviews' column to an integer, and fixes the 'neighbourhood_group' column. After applying these transformations, the script drops any remaining null values, filters the 'room_type' column to get only valid values, casts the data types of several columns to the appropriate types, and finally writes the cleaned and transformed DataFrame to an S3 bucket as Parquet files.

        self.data_clean_job = glue.CfnJob(
            self,
            "CleanRawDataJob",
            name="OtelSolution-CleanRawData",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                script_location=assets.Asset(
                    self, "CleaningJobScript", path="source/cleaning_glue_job/index.py"
                ).s3_object_url,
                python_version="3.9",
            ),
            default_arguments={
                "--enable-job-insights": "false",
                "--extra-py-files": self.xray_correlation_script.s3_object_url,
                "--xray_helper_key": self.xray_correlation_script.s3_object_key.split("/")[-1].split(".")[0],
                "--otlp_endpoint": Token.as_string(self.agent_otlp_endpoint),
                "--additional-python-modules": "boto3,opentelemetry-distro[otlp],opentelemetry-instrumentation-botocore,opentelemetry-sdk-extension-aws,opentelemetry-propagator-aws-xray,opentelemetry-instrumentation",
                "--job-language": "python",
                "library-set": "analytics"
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[self.network_connection.ref]
            ),
            max_retries=0,
            timeout=2880,
            max_capacity=1,
            glue_version="3.0",
            execution_class="STANDARD",
        )

        # The main execution reads cleaned Airbnb data from the specified S3 bucket, performs various data processing steps (calculating minimum total spend, cost per night, ranking properties by cost and reviews within neighborhoods and overall), calculates the total number of properties, creates columns for property rank based on cost and reviews, and finally writes the processed data back to the S3 bucket before committing the Glue job. Each step within the data processing is wrapped in individual spans for distributed tracing using OpenTelemetry.

        self.data_process_job = glue.CfnJob(
            self,
            "ProcessCleanDataJob",
            name="OtelSolution-ProcessCleanData",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                script_location=assets.Asset(
                    self, "ProcessingJobScript", path="source/processing_glue_job/index.py"
                ).s3_object_url,
                python_version="3.9",
            ),
            default_arguments={
                "--enable-job-insights": "false",
                "--extra-py-files": self.xray_correlation_script.s3_object_url,
                "--xray_helper_key": self.xray_correlation_script.s3_object_key.split("/")[-1].split(".")[0],
                "--otlp_endpoint": Token.as_string(self.agent_otlp_endpoint),
                "--additional-python-modules": "boto3,opentelemetry-distro[otlp],opentelemetry-instrumentation-botocore,opentelemetry-sdk-extension-aws,opentelemetry-propagator-aws-xray,opentelemetry-instrumentation",
                "--job-language": "python",
                "library-set": "analytics"
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[self.network_connection.ref]
            ),
            max_retries=0,
            timeout=2880,
            max_capacity=1,
            glue_version="3.0",
            execution_class="STANDARD",
        )

        # Step Functions State Machine
        # This state machine orchestrates the data processing pipeline for the Airbnb dataset.
        # It starts by triggering the 'CleanRawData' Glue job to clean and transform the raw data.
        # Once the cleaning job completes successfully, it triggers the 'ProcessCleanData' Glue job
        # to perform further processing and analysis on the cleaned data.
        # The state machine monitors the status of both jobs and handles success or failure scenarios accordingly.

        self.state_machine = sfn.StateMachine(
            self,
            "AirbnbDataProcessingStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(
                self.create_state_machine_definition()),
            timeout=Duration.minutes(30),
            state_machine_name="OtelSolution-DataProcessingWorkflow",
            tracing_enabled=True,
        )

        # Lambda function to trigger Step Function on Object Upload
        # https://aws-otel.github.io/docs/getting-started/lambda/lambda-python

        self._trigger_sfn_lambda = _lambda.Function(
            self,
            "TriggerStepFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            function_name="OtelSolution-InvokeStepFunction",
            handler="index.lambda_handler",
            code=_lambda.Code.from_asset("source/trigger_step_function"),
            adot_instrumentation=_lambda.AdotInstrumentationConfig(
                layer_version=_lambda.AdotLayerVersion.from_python_sdk_layer_version(
                    _lambda.AdotLambdaLayerPythonSdkVersion.LATEST
                ),
                exec_wrapper=_lambda.AdotLambdaExecWrapper.INSTRUMENT_HANDLER,
            ),
            tracing=_lambda.Tracing.ACTIVE,
            environment={
                "STEP_FUNCTION_ARN": self.state_machine.state_machine_arn,
                "AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument",
            },
            events=[
                eventsources.S3EventSource(
                    self._ingest_bucket,
                    events=[s3.EventType.OBJECT_CREATED],
                    filters=[s3.NotificationKeyFilter(
                        prefix="input/", suffix="csv")]
                )
            ],
            timeout=Duration.seconds(60),
        )

        self.state_machine.grant_start_execution(self._trigger_sfn_lambda)

    def create_state_machine_definition(self):
        # Define the Glue job start tasks
        clean_raw_data_job = tasks.GlueStartJobRun(
            self, self.data_clean_job.name,
            glue_job_name=self.data_clean_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--job_name": self.data_clean_job.ref,
                "--bucket_name.$": "$.bucket_name",
                "--object_key.$": "$.object_key",
                "--trace_id.$": "$.trace_id"
            }),
            result_path="$.CleanRawDataDetails"
        )

        process_clean_data_job = tasks.GlueStartJobRun(
            self, self.data_process_job.name,
            glue_job_name=self.data_process_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--job_name": self.data_process_job.ref,
                "--bucket_name.$": "$.bucket_name",
                "--object_key.$": "$.object_key",
                "--trace_id.$": "$.trace_id"
            }),
            result_path="$.ProcessCleanDataDetails"
        )

        definition = clean_raw_data_job\
            .next(process_clean_data_job)

        return definition
