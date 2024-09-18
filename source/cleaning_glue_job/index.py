import sys
import os
import importlib.util
import boto3
import pandas as pd
import numpy as np
import time
import random

from awsglue.utils import getResolvedOptions

# OpenTelemetry imports
from opentelemetry import trace, propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.propagators.aws import AwsXRayPropagator
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor

# Check for FUZZY_FOR_DEMO environment variable
FUZZY_FOR_DEMO = os.environ.get('FUZZY_FOR_DEMO', 'false').lower() == 'true'

def fuzzy_delay():
    if FUZZY_FOR_DEMO:
        delay = random.uniform(1, 5)
        time.sleep(delay)

def mock_s3_failure():
    if FUZZY_FOR_DEMO and random.random() < 0.1:  # 10% chance of failure
        raise Exception("Mocked S3 operation failure")

def load_xray_helper(XRAY_HELPER_KEY):
    """
    Load the X-Ray Helper Module dynamically.
    """
    print("Loading X-Ray Helper Module")
    fuzzy_delay()
    xray_helper_dir = next(d for d in sys.path if d.startswith('/tmp/glue-python-libs-'))
    xray_helper_path = os.path.join(xray_helper_dir, f'{XRAY_HELPER_KEY}.py')
    spec = importlib.util.spec_from_file_location("xray_helper", xray_helper_path)
    xray_helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(xray_helper)
    return xray_helper

def get_job_parameters():
    """
    Get job parameters from the Glue job.
    """
    fuzzy_delay()
    args = getResolvedOptions(sys.argv, [
        'job_name', 'bucket_name', 'object_key', 
        'otlp_endpoint', 'trace_id', 'xray_helper_key'
    ])
    return args

def setup_tracing(job_name, trace_id, otlp_endpoint, xray_trace):
    """
    Set up OpenTelemetry tracing.
    """
    fuzzy_delay()
    # Retrieve parent segment from X-Ray
    parent_id = xray_trace.retrieve_id(job_name)
    
    # Set up carrier for propagation
    if parent_id:
        print(f"Parent segment found: {parent_id}")
        carrier = {'X-Amzn-Trace-Id': f"Root={trace_id};Parent={parent_id};Sampled=1"}
    else:
        print("No parent segment found")
        carrier = {'X-Amzn-Trace-Id': f"Root={trace_id};Sampled=1"}

    # Set up propagator
    propagator = AwsXRayPropagator()
    propagate.set_global_textmap(propagator)
    context = propagator.extract(carrier=carrier)

    # Set up OTLP exporter
    otlp_exporter = OTLPSpanExporter(endpoint=f"http://{otlp_endpoint}:4318/v1/traces")
    span_processor = BatchSpanProcessor(otlp_exporter)

    # Set up resource attributes
    resource_attributes = {'service.name': job_name, 'cloud.provider': 'AWS::AWSGlue'}
    resource = Resource.create(attributes=resource_attributes)

    # Configure global tracer provider
    trace_provider = TracerProvider(active_span_processor=span_processor, resource=resource)
    trace.set_tracer_provider(trace_provider)

    # Setup Auto Instrumentation
    BotocoreInstrumentor().instrument(
        trace_provider = trace_provider
    )
    
    return trace.get_tracer(__name__), context

def read_data_from_s3(s3_path, tracer):
    """
    Read data from S3 using boto3 with simple retry logic.
    """
    with tracer.start_as_current_span("Read Data from S3", attributes={'S3Path': s3_path}):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                fuzzy_delay()
                mock_s3_failure()
                bucket_name, object_key = s3_path.replace("s3://", "").split("/", 1)
                s3 = boto3.client('s3')
                obj = s3.get_object(Bucket=bucket_name, Key=object_key)
                df = pd.read_csv(obj['Body'])
                print(f"Data read successfully on attempt {attempt + 1}")
                print("Data schema:")
                print(df.dtypes)
                return df, s3
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

def drop_columns(df, col_list, tracer):
    """
    Drop specified columns from the DataFrame.
    """
    with tracer.start_as_current_span("Drop Columns"):
        fuzzy_delay()
        print(f"Dropping columns: {col_list}")
        df = df.drop(columns=col_list)
    return df

def convert_currency(df, col_nm, tracer):
    """
    Convert currency column to float and drop NaN values.
    """
    with tracer.start_as_current_span("Convert Currency", attributes={'column': col_nm}):
        fuzzy_delay()
        print(f"Converting currency for column: {col_nm}")
        df[col_nm] = df[col_nm].str.replace('[$,]', '', regex=True).astype(float)
        df = df.dropna(subset=[col_nm])
    return df

def fix_skewness(df, col_name, tracer):
    """
    Fix skewness in the specified column using square root transformation.
    """
    with tracer.start_as_current_span("Fix Skewness", attributes={'column': col_name}):
        fuzzy_delay()
        print(f"Fixing skewness for column: {col_name}")
        df[col_name] = df[col_name].astype(float)
        median_value = df[col_name].median()
        print(f"Median value for {col_name}: {median_value}")
        df[col_name] = df[col_name].fillna(median_value)
        df[col_name] = np.sqrt(df[col_name])
    return df

def correct_review_dates(df, col_nm, tracer):
    """
    Correct review dates in the specified column.
    """
    with tracer.start_as_current_span("Correct Review Dates", attributes={'column': col_nm}):
        fuzzy_delay()
        print(f"Correcting review dates for column: {col_nm}")
        
        # Convert to datetime, coercing errors to NaT
        df['date_formatted'] = pd.to_datetime(df[col_nm], format='%m/%d/%y', errors='coerce')
        
        # Convert to Unix timestamp (seconds since epoch)
        df['date_formatted_epoch'] = df['date_formatted'].astype(int) // 10**9
        
        # Define the condition for invalid dates
        current_year = pd.Timestamp.now().year
        condition = (df['date_formatted'].dt.year > current_year) | (df['date_formatted'].dt.year < df['construction_year'])
        
        # Calculate median value for valid dates
        valid_dates = df.loc[~condition, 'date_formatted_epoch']
        median_val = valid_dates.median()
        print(f"Median epoch value for review dates: {median_val}")
        
        # Replace invalid dates with median
        df['abc_new'] = df['date_formatted_epoch'].where(~condition, median_val)
        df['abc_new'] = df['abc_new'].fillna(median_val)
        
        # Convert back to datetime
        df['last_review_final'] = pd.to_datetime(df['abc_new'], unit='s', errors='coerce')
        
        # Drop intermediate columns and rename
        df = df.drop(columns=[col_nm, 'date_formatted', 'date_formatted_epoch', 'abc_new'])
        df = df.rename(columns={'last_review_final': 'last_review'})
    
    return df

def process_data(df, tracer):
    """
    Main data processing function.
    """
    with tracer.start_as_current_span("Data Processing"):
        fuzzy_delay()
        # Standardizing column names
        df.columns = [x.lower().replace(' ', '_') for x in df.columns]
        print("Standardized column names:", df.columns)

        # Dropping unwanted columns
        col_list = ["host_id", "country_code", "license", "availability_365"]
        df = drop_columns(df, col_list, tracer)

        # Removing duplicates
        print("Removing duplicates")
        df = df.drop_duplicates(subset=['host_name', 'lat', 'long', 'price'])

        # Filling in null values
        print("Filling null values")
        null_fill_map = {
            'house_rules': 'Blank',
            'host_name': 'Blank',
            'name': 'Blank',
            'country': 'United States',
            'host_identity_verified': 'Unconfirmed',
            'instant_bookable': 'TRUE',
            'cancellation_policy': 'moderate'
        }
        df = df.fillna(null_fill_map)

        # Filtering the minimum nights
        print("Filtering minimum nights")
        df = df[(df['minimum_nights'] > 0) & (df['minimum_nights'] < 365)]

        # Round the number of reviews
        print("Rounding number of reviews")
        df['number_of_reviews'] = df['number_of_reviews'].round()

        # Converting column types
        df = convert_currency(df, 'price', tracer)
        df = convert_currency(df, 'service_fee', tracer)

        # Fixing skewness
        for col in ['reviews_per_month', 'number_of_reviews', 'calculated_host_listings_count']:
            df = fix_skewness(df, col, tracer)

        # Fixing review dates
        df = correct_review_dates(df, 'last_review', tracer)

        # Correcting neighborhood name
        print("Correcting neighborhood name")
        df['neighbourhood_group'] = df['neighbourhood_group'].replace('brookln', 'Brooklyn')

        # Dropping remaining null values
        print("Dropping remaining null values")
        df = df.dropna()

        # Filtering room type
        print("Filtering room type")
        df = df[df['room_type'].str.len() > 4]

        # Fixing column datatypes
        print("Fixing column datatypes")
        df['id'] = range(1, len(df) + 1)
        int_columns = ['price', 'service_fee', 'minimum_nights', 'number_of_reviews', 
                       'reviews_per_month', 'calculated_host_listings_count']
        df[int_columns] = df[int_columns].round().astype(int)

    return df

def write_data_to_s3(df, bucket_name, s3, tracer):
    """
    Write processed data to S3 as a Parquet file with simple retry logic.
    """
    output_s3_path = f"s3://{bucket_name}/cleaned/output.parquet"
    print(f"Writing processed data to S3: {output_s3_path}")
    with tracer.start_as_current_span("Write Data to S3", attributes={'S3Path': output_s3_path}):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                fuzzy_delay()
                mock_s3_failure()
                buffer = df.to_parquet()
                s3.put_object(Bucket=bucket_name, Key='cleaned/output.parquet', Body=buffer)
                print(f"Data written successfully on attempt {attempt + 1}")
                return
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

def main():
    # Get job parameters
    args = get_job_parameters()
    JOB_NAME = args['job_name']
    SOURCE_BUCKET_NAME = args['bucket_name']
    SOURCE_OBJECT_KEY = args['object_key']
    TRACE_ID = args['trace_id']
    OTLP_ENDPOINT = args['otlp_endpoint']
    XRAY_HELPER_KEY = args['xray_helper_key']

    # Load X-Ray Helper
    xray_helper = load_xray_helper(XRAY_HELPER_KEY)

    # Setup tracing
    xray_trace = xray_helper.XRayTrace(TRACE_ID)
    tracer, context = setup_tracing(JOB_NAME, TRACE_ID, OTLP_ENDPOINT, xray_trace)

    # Main job execution
    print("Starting Glue Job Execution")
    with tracer.start_as_current_span("Glue Job Execution", context=context, kind=trace.SpanKind.SERVER, attributes={'job_name': JOB_NAME}):
        # Read data from S3
        s3_path = f"s3://{SOURCE_BUCKET_NAME}/{SOURCE_OBJECT_KEY}"
        df, s3 = read_data_from_s3(s3_path, tracer)

        # Process data
        df = process_data(df, tracer)

        # Write processed data to S3
        write_data_to_s3(df, SOURCE_BUCKET_NAME, s3, tracer)

    print("Glue job execution completed")

if __name__ == "__main__":
    main()