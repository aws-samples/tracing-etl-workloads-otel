import sys
import os
import importlib.util
import logging
import boto3
import io
import pandas as pd
import numpy as np

from awsglue.utils import getResolvedOptions
from opentelemetry import propagate, trace
from opentelemetry.propagators.aws import AwsXRayPropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# Constants
REQUIRED_ARGS = ['job_name', 'bucket_name', 'otlp_endpoint', 'trace_id', 'xray_helper_key']

def get_job_parameters():
    """
    Retrieve job parameters from Glue Job arguments.
    """
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    return args

def load_xray_helper(xray_helper_key):
    """
    Load the X-Ray Helper Module dynamically.
    """
    print("Loading X-Ray Helper Module")
    xray_helper_dir = next(d for d in sys.path if d.startswith('/tmp/glue-python-libs-'))
    xray_helper_path = os.path.join(xray_helper_dir, f'{xray_helper_key}.py')
    spec = importlib.util.spec_from_file_location("xray_helper", xray_helper_path)
    xray_helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(xray_helper)
    return xray_helper

def setup_tracing(job_name, trace_id, otlp_endpoint, xray_helper):
    """
    Set up OpenTelemetry tracing.
    """
    # Set up resource attributes
    resource_attributes = {'service.name': job_name, 'cloud.provider': 'AWS::AWSGlue'}
    resource = Resource.create(attributes=resource_attributes)

    # Set up OTLP exporter and BatchSpanProcessor
    otlp_exporter = OTLPSpanExporter(endpoint=f"http://{otlp_endpoint}:4318/v1/traces")
    processor = BatchSpanProcessor(otlp_exporter)

    # Configure global tracer provider
    tracer_provider = TracerProvider(resource=resource, active_span_processor=processor)
    trace.set_tracer_provider(tracer_provider)
    tracer = trace.get_tracer(__name__)

    # Retrieve X-Ray trace and parent context
    print("Retrieving XRay trace")
    xray_trace = xray_helper.XRayTrace(trace_id)
    parent_id = xray_trace.retrieve_id(job_name)

    if parent_id:
        print(f"Parent segment found: {parent_id}")
        carrier = {'X-Amzn-Trace-Id': f"Root={trace_id};Parent={parent_id};Sampled=1"}
        propagator = AwsXRayPropagator()
        propagate.set_global_textmap(propagator)
        context = propagator.extract(carrier=carrier)
    else:
        print("No parent segment found")
        context = None

    return tracer, context

def read_data_from_s3(bucket_name, tracer):
    """
    Read data from S3 using boto3.
    """
    with tracer.start_as_current_span("Read from S3"):
        s3 = boto3.client('s3')
        input_path = f'{bucket_name}/cleaned/output.parquet'
        obj = s3.get_object(Bucket=bucket_name, Key='cleaned/output.parquet')
        
        parquet_buffer = io.BytesIO(obj['Body'].read())
        df = pd.read_parquet(parquet_buffer)
    
    print(f"Created DataFrame with {len(df)} rows")
    return df, s3

def process_data(df, tracer):
    """
    Process the data with various calculations and rankings.
    """
    with tracer.start_as_current_span("Data Processing"):
        with tracer.start_as_current_span("Calculate Minimum Total Spend"):
            print("Calculating minimum total spend")
            df['minimum_total_spend'] = (df['price'] + df['service_fee']) * df['minimum_nights']
        
        with tracer.start_as_current_span("Calculate Cost Per Night"):
            print("Calculating cost per night")
            df['cost_per_night'] = df['price'] + df['service_fee']
        
        with tracer.start_as_current_span("Rank by Cost Per Night in Neighborhood"):
            print("Ranking by cost per night in neighborhood")
            df['exp_rank_per_neighbourhood'] = df.groupby('neighbourhood')['cost_per_night'].rank(method='dense', ascending=False)
        
        with tracer.start_as_current_span("Rank by Cost Per Night Overall"):
            print("Ranking by cost per night overall")
            df['exp_rank_overall'] = df['cost_per_night'].rank(method='dense', ascending=False)
        
        with tracer.start_as_current_span("Rank by Number of Reviews"):
            print("Ranking by number of reviews")
            df['rank_overall_reviews'] = df['number_of_reviews'].rank(method='dense', ascending=False)
        
        with tracer.start_as_current_span("Calculate Total Properties"):
            print("Calculating total number of properties")
            total_no_props = len(df)
            print(f"Total number of properties: {total_no_props}")
        
        with tracer.start_as_current_span("Create Property Rank Columns"):
            print("Creating property rank columns")
            df['prop_rank_exp'] = df['exp_rank_overall'].astype(str) + '/' + str(total_no_props)
            df['prop_rank_review'] = df['rank_overall_reviews'].astype(str) + '/' + str(total_no_props)
    
    return df

def write_data_to_s3(df, bucket_name, s3, tracer):
    """
    Write processed data to S3 as a Parquet file.
    """
    with tracer.start_as_current_span("Write to S3"):
        output_path = f'{bucket_name}/processed/output.parquet'
        buffer = df.to_parquet()
        s3.put_object(Bucket=bucket_name, Key='processed/output.parquet', Body=buffer)

def main():
    # Get job parameters
    args = get_job_parameters()
    job_name = args['job_name']
    bucket_name = args['bucket_name']
    trace_id = args['trace_id']
    otlp_endpoint = args['otlp_endpoint']
    xray_helper_key = args['xray_helper_key']

    # Load X-Ray Helper
    xray_helper = load_xray_helper(xray_helper_key)

    # Setup tracing
    tracer, context = setup_tracing(job_name, trace_id, otlp_endpoint, xray_helper)

    # Main job execution
    print("Starting main execution")
    with tracer.start_as_current_span("Glue Job Execution", context=context, kind=trace.SpanKind.SERVER, attributes={'job_name': job_name}):
        # Read data from S3
        df, s3 = read_data_from_s3(bucket_name, tracer)

        # Process data
        df = process_data(df, tracer)

        # Write processed data to S3
        write_data_to_s3(df, bucket_name, s3, tracer)

    print("Glue job execution completed")

if __name__ == "__main__":
    main()