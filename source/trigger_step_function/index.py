import json
import boto3
import os
from typing import Dict

# Create Boto3 clients (reusable across multiple invocations)
s3_client = boto3.client('s3')
stepfunctions_client = boto3.client('stepfunctions')

def lambda_handler(event, context):
    try:
        print(json.dumps(event))

        # Validate the incoming event
        if 'Records' not in event or not event['Records']:
            print("Invalid event structure. No 'Records' found.")
            return {
                'statusCode': 400,
                'body': 'Invalid event structure'
            }

        # Extract S3 event information
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        object_key = s3_event['object']['key']

        print(f"S3 Event - Bucket: {bucket_name}, Object Key: {object_key}")

        # Prepare input payload for Step Function
        trace_id = os.environ.get("_X_AMZN_TRACE_ID")
        if trace_id:
            trace_id = trace_id.split(";")[0].split("=")[1]
        else:
            logger.warning("_X_AMZN_TRACE_ID environment variable not found. Skipping trace ID in input payload.")

        input_payload: Dict[str, str] = {
            'bucket_name': bucket_name,
            'object_key': object_key
        }
        if trace_id:
            input_payload['trace_id'] = trace_id

        # Get Step Function ARN from environment variable
        step_function_arn = os.environ.get('STEP_FUNCTION_ARN')
        if not step_function_arn:
            print("STEP_FUNCTION_ARN environment variable not found.")
            return {
                'statusCode': 500,
                'body': 'Internal server error'
            }

        # Invoke Step Function
        response = stepfunctions_client.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps(input_payload)
        )

        execution_arn = response['executionArn']
        print(f"Step Function execution started: {execution_arn}")

        return {
            'statusCode': 200,
            'body': f'Step Function execution started: {execution_arn}'
        }

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'An unexpected error occurred'
        }