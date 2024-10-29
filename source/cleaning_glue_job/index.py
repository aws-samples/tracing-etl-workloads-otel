# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import os
import importlib.util
import random
import time
import boto3
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions

# OpenTelemetry imports
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.propagators.aws import AwsXRayPropagator
from opentelemetry.trace.status import Status, StatusCode

# Constants
REQUIRED_ARGS = ['job_name', 'bucket_name', 'object_key', 'otlp_endpoint', 'trace_id', 'xray_helper_key']
FUZZY_FOR_DEMO = True  # Set this to False in production

def fuzzy_delay():
    if FUZZY_FOR_DEMO:
        delay = random.uniform(1, 5)  # nosec B311
        time.sleep(delay)

def mock_s3_failure():
    if FUZZY_FOR_DEMO and random.random() < 0.3:  # nosec B311
        error_response = {
            'Error': {
                'Code': 'NoSuchKey',
                'Message': 'The specified key does not exist.'
            }
        }
        raise ClientError(error_response, 'GetObject')

def get_job_parameters():
    return getResolvedOptions(sys.argv, REQUIRED_ARGS)

def load_xray_helper(xray_helper_key):
    fuzzy_delay()
    xray_helper_dir = next(d for d in sys.path if d.startswith('/tmp/glue-python-libs-'))  # nosec B108
    xray_helper_path = os.path.join(xray_helper_dir, f'{xray_helper_key}.py')
    spec = importlib.util.spec_from_file_location("xray_helper", xray_helper_path)
    xray_helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(xray_helper)
    return xray_helper

def setup_tracing(job_name, trace_id, otlp_endpoint, xray_helper):
    resource = Resource.create({'service.name': job_name, 'cloud.provider': 'AWS::AWSGlue'})
    otlp_exporter = OTLPSpanExporter(endpoint=f"http://{otlp_endpoint}:4318/v1/traces")
    processor = BatchSpanProcessor(otlp_exporter)
    trace.set_tracer_provider(TracerProvider(resource=resource, active_span_processor=processor))
    tracer = trace.get_tracer(__name__)

    xray_trace = xray_helper.XRayTrace(trace_id)
    parent_id = xray_trace.retrieve_id(job_name)

    if parent_id:
        carrier = {'X-Amzn-Trace-Id': f"Root={trace_id};Parent={parent_id};Sampled=1"}
        propagator = AwsXRayPropagator()
        context = propagator.extract(carrier=carrier)
    else:
        context = None

    return tracer, context

def read_data_from_s3(s3_path, tracer):
    """
    Read data from S3 using boto3 with simple retry logic.
    """
    with tracer.start_as_current_span("Read Data from S3", attributes={'S3Path': s3_path}) as span:
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
                span.set_status(Status(StatusCode.OK))
                return df, s3
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                span.record_exception(e)
                if attempt == max_retries - 1:
                    span.set_status(Status(StatusCode.ERROR))
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

def drop_columns(df, col_list, tracer):
    with tracer.start_as_current_span("Drop Columns"):
        print(f"Dropping columns: {col_list}")
        df = df.drop(columns=col_list)
    return df

def convert_currency(df, col_nm, tracer):
    with tracer.start_as_current_span("Convert Currency", attributes={'column': col_nm}):
        print(f"Converting currency for column: {col_nm}")
        df[col_nm] = df[col_nm].str.replace('[$,]', '', regex=True).astype(float)
        df = df.dropna(subset=[col_nm])
    return df

def fix_skewness(df, col_name, tracer):
    with tracer.start_as_current_span("Fix Skewness", attributes={'column': col_name}):
        print(f"Fixing skewness for column: {col_name}")
        df[col_name] = df[col_name].astype(float)
        median_value = df[col_name].median()
        print(f"Median value for {col_name}: {median_value}")
        df[col_name] = df[col_name].fillna(median_value)
        df[col_name] = np.sqrt(df[col_name])
    return df

def correct_review_dates(df, col_nm, tracer):
    with tracer.start_as_current_span("Correct Review Dates", attributes={'column': col_nm}):
        print(f"Correcting review dates for column: {col_nm}")
        df['date_formatted'] = pd.to_datetime(df[col_nm], format='%m/%d/%y', errors='coerce')
        df['date_formatted_epoch'] = df['date_formatted'].astype(int) // 10**9
        current_year = pd.Timestamp.now().year
        condition = (df['date_formatted'].dt.year > current_year) | (df['date_formatted'].dt.year < df['construction_year'])
        valid_dates = df.loc[~condition, 'date_formatted_epoch']
        median_val = valid_dates.median()
        print(f"Median epoch value for review dates: {median_val}")
        df['abc_new'] = df['date_formatted_epoch'].where(~condition, median_val)
        df['abc_new'] = df['abc_new'].fillna(median_val)
        df['last_review_final'] = pd.to_datetime(df['abc_new'], unit='s', errors='coerce')
        df = df.drop(columns=[col_nm, 'date_formatted', 'date_formatted_epoch', 'abc_new'])
        df = df.rename(columns={'last_review_final': 'last_review'})
    return df

def process_data(df, tracer):
    with tracer.start_as_current_span("Data Processing"):
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
    with tracer.start_as_current_span("Write Data to S3", attributes={'S3Path': output_s3_path}) as span:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                fuzzy_delay()
                mock_s3_failure()
                print(f"Writing processed data to S3: {output_s3_path}")
                buffer = df.to_parquet()
                s3.put_object(Bucket=bucket_name, Key='cleaned/output.parquet', Body=buffer)
                print(f"Data written successfully on attempt {attempt + 1}")
                span.set_status(Status(StatusCode.OK))
                return
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                span.record_exception(e)
                if attempt == max_retries - 1:
                    span.set_status(Status(StatusCode.ERROR))
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

def main():
    args = get_job_parameters()
    JOB_NAME = args['job_name']
    SOURCE_BUCKET_NAME = args['bucket_name']
    SOURCE_OBJECT_KEY = args['object_key']
    TRACE_ID = args['trace_id']
    OTLP_ENDPOINT = args['otlp_endpoint']
    XRAY_HELPER_KEY = args['xray_helper_key']

    xray_helper = load_xray_helper(XRAY_HELPER_KEY)
    tracer, context = setup_tracing(JOB_NAME, TRACE_ID, OTLP_ENDPOINT, xray_helper)

    with tracer.start_as_current_span("Glue Job Execution", context=context, kind=trace.SpanKind.SERVER, attributes={'job_name': JOB_NAME}) as main_span:
        try:
            s3_path = f"s3://{SOURCE_BUCKET_NAME}/{SOURCE_OBJECT_KEY}"
            df, s3 = read_data_from_s3(s3_path, tracer)
            df = process_data(df, tracer)
            write_data_to_s3(df, SOURCE_BUCKET_NAME, s3, tracer)
            main_span.set_status(Status(StatusCode.OK))
        except Exception as e:
            main_span.set_status(Status(StatusCode.ERROR))
            main_span.record_exception(e)
            print(f"Job execution failed: {str(e)}")
            raise

    print("Glue job execution completed")

if __name__ == "__main__":
    main()