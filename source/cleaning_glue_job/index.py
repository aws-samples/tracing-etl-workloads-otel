import sys
import os
import importlib.util
import boto3
import pandas as pd
import numpy as np
from datetime import datetime
import re

from awsglue.utils import getResolvedOptions

from opentelemetry import propagate, trace
from opentelemetry.propagators.aws import AwsXRayPropagator
from opentelemetry.trace import set_span_in_context
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.span import NonRecordingSpan
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# Get job parameters
args = getResolvedOptions(sys.argv, ['job_name', 'bucket_name', 'object_key', 'otlp_endpoint', 'trace_id', 'xray_helper_key'])

JOB_NAME = args.get('job_name', None)
SOURCE_BUCKET_NAME = args.get('bucket_name', None)
SOURCE_OBJECT_KEY = args.get('object_key', None)
TRACE_ID = args.get('trace_id', None)
OTLP_ENDPOINT = args.get('otlp_endpoint', None)
XRAY_HELPER_KEY = args.get('xray_helper_key', None)

# Construct the full path to the XRay helper file
xray_helper_dir = next(d for d in sys.path if d.startswith('/tmp/glue-python-libs-'))
xray_helper_path = os.path.join(xray_helper_dir, f'{XRAY_HELPER_KEY}.py')
spec = importlib.util.spec_from_file_location("xray_helper", xray_helper_path)
xray_helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(xray_helper)

# Instrumentation Setup
tracer = trace.get_tracer(__name__)
resource_attributes = {'service.name': JOB_NAME, 'cloud.provider': 'AWS::AWSGlue'}
resource = Resource.create(attributes=resource_attributes)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=f"http://{OTLP_ENDPOINT}:4318/v1/traces"))
tracer_provider = TracerProvider(resource=resource, active_span_processor=processor)
trace.set_tracer_provider(tracer_provider)

print("Retrieving XRay trace")
xray_trace = xray_helper.XRayTrace(TRACE_ID)
parent_id = xray_trace.retrieve_id(JOB_NAME)

if parent_id:
    print(f"Parent segment found: {parent_id}")
    carrier = {'X-Amzn-Trace-Id': f"Root={TRACE_ID};Parent={parent_id};Sampled=1"}
    propagator = AwsXRayPropagator()
    context = propagator.extract(carrier=carrier)
else:
    sys.exit("Parent segment not found. Tracing will not be nested correctly.")

# Processing Data-set
print("Starting Glue Job Execution")
with tracer.start_as_current_span("Glue Job Execution", context=context, kind=trace.SpanKind.SERVER, attributes={'job_name': JOB_NAME}):
    # Read data from S3
    s3_path = f"s3://{SOURCE_BUCKET_NAME}/{SOURCE_OBJECT_KEY}"
    print(f"Reading data from S3: {s3_path}")
    with tracer.start_as_current_span("Read Data", attributes={'S3Path': s3_path}):
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=SOURCE_BUCKET_NAME, Key=SOURCE_OBJECT_KEY)
        df = pd.read_csv(obj['Body'])
        print("Data schema:")
        print(df.dtypes)

    # Data Processing Functions
    def drop_columns(df, col_list):
        print(f"Dropping columns: {col_list}")
        with tracer.start_as_current_span("Drop Columns"):
            df = df.drop(columns=col_list)
        return df

    def con_curr(df, col_nm):
        print(f"Converting currency for column: {col_nm}")
        with tracer.start_as_current_span("Convert Currency", attributes={'column': col_nm}):
            df[col_nm] = df[col_nm].str.replace('[$,]', '', regex=True).astype(float)
            df = df.dropna(subset=[col_nm])
        return df

    def fix_skew(df, col_name):
        print(f"Fixing skewness for column: {col_name}")
        with tracer.start_as_current_span("Fix Skewness", attributes={'column': col_name}):
            df[col_name] = df[col_name].astype(float)
            median_value = df[col_name].median()
            print(f"Median value for {col_name}: {median_value}")
            df[col_name] = df[col_name].fillna(median_value)
            df[col_name] = np.sqrt(df[col_name])
        return df

    def correct_review(df, col_nm):
        print(f"Correcting review dates for column: {col_nm}")
        with tracer.start_as_current_span("Correct Review Dates", attributes={'column': col_nm}):
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

    # Data Processing
    print("Starting data processing")
    with tracer.start_as_current_span("Data Processing"):
        # Standardizing column names
        df.columns = [x.lower().replace(' ', '_') for x in df.columns]
        print("Standardized column names:", df.columns)

        # Dropping unwanted columns
        col_list = ["host_id", "country_code", "license", "availability_365"]
        df = drop_columns(df, col_list)

        # Removing duplicates
        print("Removing duplicates")
        df = df.drop_duplicates(subset=['host_name', 'lat', 'long', 'price'])

        # Filling in null values
        print("Filling null values")
        df['house_rules'] = df['house_rules'].fillna('Blank')
        df['host_name'] = df['host_name'].fillna('Blank')
        df['name'] = df['name'].fillna('Blank')
        df['country'] = df['country'].fillna('United States')
        df['host_identity_verified'] = df['host_identity_verified'].fillna('Unconfirmed')
        df['instant_bookable'] = df['instant_bookable'].fillna('TRUE')
        df['cancellation_policy'] = df['cancellation_policy'].fillna('moderate')

        # Filtering the minimum nights
        print("Filtering minimum nights")
        df = df[(df['minimum_nights'] > 0) & (df['minimum_nights'] < 365)]

        # Round the number of reviews
        print("Rounding number of reviews")
        df['number_of_reviews'] = df['number_of_reviews'].round()

        # Converting column types
        df = con_curr(df, 'price')
        df = con_curr(df, 'service_fee')

        # Fixing skewness
        df = fix_skew(df, 'reviews_per_month')
        df = fix_skew(df, 'number_of_reviews')
        df = fix_skew(df, 'calculated_host_listings_count')

        # Fixing review dates
        df = correct_review(df, 'last_review')

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
        df['price'] = df['price'].astype(int)
        df['service_fee'] = df['service_fee'].astype(int)
        df['minimum_nights'] = df['minimum_nights'].astype(int)
        df['number_of_reviews'] = df['number_of_reviews'].astype(int)
        df['reviews_per_month'] = df['reviews_per_month'].round().astype(int)
        df['calculated_host_listings_count'] = df['calculated_host_listings_count'].round().astype(int)

    # Writing processed data to S3
    output_s3_path = f"s3://{SOURCE_BUCKET_NAME}/cleaned/output.parquet"
    print(f"Writing processed data to S3: {output_s3_path}")
    with tracer.start_as_current_span("Write Data to S3", attributes={'S3Path': output_s3_path}):
        buffer = df.to_parquet()
        s3.put_object(Bucket=SOURCE_BUCKET_NAME, Key='cleaned/output.parquet', Body=buffer)

print("Glue job execution completed")