# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json

class XRayTrace:
    """
    A class to represent an X-Ray trace and provide methods for trace analysis.
    """
    def __init__(self, trace_id):
        """
        Initialize an XRayTrace object with the provided trace ID.

        Args:
            trace_id (str): The ID of the X-Ray trace.
        """
        self.trace_id = trace_id
        self.trace = self._retrieve_trace()

    def _retrieve_trace(self):
        """
        Retrieve the X-Ray trace using the AWS X-Ray API.

        Returns:
            dict: The retrieved X-Ray trace.
        """
        xray = boto3.client('xray')
        trace = xray.batch_get_traces(TraceIds=[self.trace_id])
        trace = trace['Traces'][0]
        for segment in trace['Segments']:
            segment['Document'] = json.loads(segment['Document'])
        return trace

    def find_segment_by_origin(self, origin):
        """
        Find a segment in the trace based on the provided origin.

        Args:
            origin (str): The origin of the segment to find.

        Returns:
            dict: The found segment, or None if not found.
        """
        for segment in self.trace['Segments']:
            if segment['Document'].get('origin') == origin:
                return segment
        return None

    def find_segment_by_request_id(self, request_id):
        """
        Find a segment in the trace based on the provided request ID.

        Args:
            request_id (str): The request ID of the segment to find.

        Returns:
            str: The ID of the found segment, or None if not found.
        """
        for segment in self.trace['Segments']:
            if segment['Document'].get('aws', {}).get('request_id') == request_id:
                return segment['Id']
        return None

    def find_step_subsegment(self, segment, step_name):
        """
        Find a step subsegment within the provided segment based on the step name.

        Args:
            segment (dict): The segment to search for the step subsegment.
            step_name (str): The name of the step subsegment to find.

        Returns:
            dict: The found step subsegment, or None if not found.
        """
        for subsegment in segment['Document'].get('subsegments', []):
            if subsegment['name'] == step_name:
                return subsegment
        return None

    def find_aws_sdk_subsegment(self, subsegment):
        """
        Find an AWS SDK subsegment within the provided subsegment.

        Args:
            subsegment (dict): The subsegment to search for the AWS SDK subsegment.

        Returns:
            str: The request ID of the found AWS SDK subsegment, or None if not found.
        """
        for sub in subsegment.get('subsegments', []):
            if 'aws' in sub:
                return sub['aws']['request_id']
        return None

    def retrieve_id(self, step_name):

        stepfunctions_segment = self.find_segment_by_origin('AWS::StepFunctions::StateMachine')

        if stepfunctions_segment:
            print(f"Found StepFunctions segment ID: {stepfunctions_segment['Id']}")
        else:
            print(f"StepFunctions segment not found")
            return None

        step_subsegment = self.find_step_subsegment(stepfunctions_segment, step_name)
        if step_subsegment:
            print(f"Found step subsegment: {step_name}")
        else:
            print(f"Step subsegment not found for step: {step_name}")
            return None

        aws_sdk_request_id = self.find_aws_sdk_subsegment(step_subsegment)
        if aws_sdk_request_id:
            print(f"Found AWS SDK request ID: {aws_sdk_request_id}")
        else:
            print(f"AWS SDK request ID not found for step: {step_name}")
            return None

        aws_sdk_segment_id = self.find_segment_by_request_id(aws_sdk_request_id)
        if aws_sdk_segment_id:
            print(f"Found AWS SDK segment ID: {aws_sdk_segment_id}")
            return aws_sdk_segment_id
        else:
            print(f"AWS SDK segment not found for request ID: {aws_sdk_request_id}")
            return None
