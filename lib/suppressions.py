from cdk_nag import NagSuppressions

def add_suppressions(stack):
    NagSuppressions.add_stack_suppressions(stack, [
        {
            "id": "AwsSolutions-VPC7",
            "reason": "VPC Flow Logs are disabled for this demo to reduce costs and simplify the setup. In a production environment, VPC Flow Logs should be enabled for network monitoring and security analysis."
        },
        {
            "id": "AwsSolutions-ECS4",
            "reason": "CloudWatch Container Insights are disabled for this demo to minimize operational overhead. For production workloads, enabling Insights would provide valuable performance and diagnostic data for ECS clusters."
        },
        {
            "id": "AwsSolutions-IAM4",
            "reason": "AWS managed policies are used in this demo for simplicity and to showcase standard roles. In a production setting, it's recommended to create custom IAM policies that adhere to the principle of least privilege."
        },
        {
            "id": "AwsSolutions-IAM5",
            "reason": "Wildcard permissions are used in this demo for various services (Glue, Step Functions, Lambda, S3) to simplify IAM setup and demonstrate functionality. In a production environment, these permissions should be scoped down to specific resources and actions based on the principle of least privilege."
        },
        {
            "id": "AwsSolutions-S1",
            "reason": "S3 server access logging is disabled for this demo to reduce complexity and storage costs. In a production environment, enabling access logs is crucial for security auditing and compliance requirements."
        },
        {
            "id": "AwsSolutions-S10",
            "reason": "SSL-only access is not enforced on the S3 bucket in this demo for easier testing and data access. In a real-world scenario, enforcing SSL should be mandatory to ensure data in transit is encrypted."
        },
        {
            "id": "AwsSolutions-GL1",
            "reason": "CloudWatch Log encryption is not enabled for Glue jobs in this demo to simplify the setup. For production use, encrypting logs is important to protect potentially sensitive information captured in job logs."
        },
        {
            "id": "AwsSolutions-GL3",
            "reason": "Job bookmark encryption is not used in this demo Glue job to keep the configuration straightforward. In a production ETL pipeline, enabling this feature would be important for data consistency and security."
        },
        {
            "id": "AwsSolutions-SF1",
            "reason": "Full logging to CloudWatch Logs is not enabled for the Step Function in this demo to reduce log volume and simplify analysis. In a production workflow, comprehensive logging would be crucial for debugging and auditing."
        },
        {
            "id": "AwsSolutions-L1",
            "reason": "The latest runtime version is not enforced for the Lambda function in this demo to ensure compatibility with the provided code samples. In a production environment, using the latest runtime is recommended for security patches and performance improvements."
        },
        {
            "id": "AwsSolutions-EC23",
            "reason": "Open security group rules are used in this demo for simplicity. In a production environment, security group rules should be restricted to specific IP ranges or security groups."
        },
        {
            "id": "AwsSolutions-EC27",
            "reason": "Security group descriptions are omitted in this demo for brevity. In a production environment, meaningful descriptions should be provided for all security groups to aid in management and auditing."
        },
        {
            "id": "AwsSolutions-ECS2",
            "reason": "Environment variables are directly specified in task definitions for this demo. In a production environment, sensitive information should be stored securely and referenced indirectly."
        },
        {
            "id": "AwsSolutions-ECS7",
            "reason": "Container logging might not be enabled for all containers in this demo to simplify the setup. In a production environment, logging should be enabled for all containers for proper monitoring and troubleshooting."
        },
        {
            "id": "AwsSolutions-SF2",
            "reason": "X-Ray tracing might not be enabled for Step Functions in this demo. In a production environment, enabling tracing provides valuable insights for performance analysis and troubleshooting."
        }
    ])