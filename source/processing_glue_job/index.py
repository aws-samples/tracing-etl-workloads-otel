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
from opentelemetry.trace import set_span_in_context
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.span import NonRecordingSpan
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# Retrieve S3 input and output paths from Glue Job arguments
args = getResolvedOptions(sys.argv, ['job_name', 'bucket_name', 'otlp_endpoint', 'trace_id', 'xray_helper_key'])

JOB_NAME = args.get('job_name', None)
SOURCE_BUCKET_NAME = args.get('bucket_name', None)
TRACE_ID = args.get('trace_id', None)
OTLP_ENDPOINT = args.get('otlp_endpoint', None)
XRAY_HELPER_KEY = args.get('xray_helper_key', None)

# Prepare X-Ray Helper
xray_helper_dir = next(d for d in sys.path if d.startswith('/tmp/glue-python-libs-'))
xray_helper_path = os.path.join(xray_helper_dir, f'{XRAY_HELPER_KEY}.py')
spec = importlib.util.spec_from_file_location("xray_helper", xray_helper_path)
xray_helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(xray_helper)

# Instrumentation Setup
tracer = trace.get_tracer(__name__)
resource_attributes = { 'service.name': JOB_NAME, 'cloud.provider': 'AWS::AWSGlue'}
resource = Resource.create(attributes=resource_attributes)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=f"http://{OTLP_ENDPOINT}:4318/v1/traces"))
tracer_provider = TracerProvider(resource=resource, active_span_processor=processor)
trace.set_tracer_provider(tracer_provider)

print("Retrieving XRay trace")
xray_trace = xray_helper.XRayTrace(TRACE_ID)
parent_id = xray_trace.retrieve_id(JOB_NAME)

if parent_id:
    print(f"Parent segment found: {parent_id}")
    carrier = {'X-Amzn-Trace-Id': f"Root={xray_trace.trace_id};Parent={parent_id};Sampled=1"}
    propagator = AwsXRayPropagator()
    context = propagator.extract(carrier=carrier)
else:
    sys.exit("Parent segment not found. Tracing will not be nested correctly.")

print("Starting main execution")
with tracer.start_as_current_span("Glue Job Execution", context=context, kind=trace.SpanKind.SERVER, attributes={'job_name': JOB_NAME}):
    # Read data
    print("Reading input data")
    with tracer.start_as_current_span("Read from S3"):
        s3 = boto3.client('s3')
        input_path = f'{SOURCE_BUCKET_NAME}/cleaned/output.parquet'
        obj = s3.get_object(Bucket=SOURCE_BUCKET_NAME, Key='cleaned/output.parquet')
        
        # Create a BytesIO object from the S3 object's body
        parquet_buffer = io.BytesIO(obj['Body'].read())
        
        # Read the Parquet file from the BytesIO object
        df = pd.read_parquet(parquet_buffer)
    
    print(f"Created DataFrame with {len(df)} rows")

    # Data Processing
    print("Starting data processing")
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

    # Write processed data
    print("Writing processed data")
    with tracer.start_as_current_span("Write to S3"):
        output_path = f'{SOURCE_BUCKET_NAME}/processed/output.parquet'
        buffer = df.to_parquet()
        s3.put_object(Bucket=SOURCE_BUCKET_NAME, Key='processed/output.parquet', Body=buffer)

print("Glue job execution completed")