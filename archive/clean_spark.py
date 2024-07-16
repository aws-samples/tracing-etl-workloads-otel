import sys
import os
import importlib.util

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

from opentelemetry import propagate, trace
from opentelemetry.propagators.aws import AwsXRayPropagator
from opentelemetry.trace import set_span_in_context
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.span import NonRecordingSpan
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

args = getResolvedOptions(sys.argv, ['job_name', 'bucket_name','object_key', 'otlp_endpoint', 'trace_id', 'xray_helper_key'])

JOB_NAME = args.get('job_name', None)
JOB_RUN_ID = args['JOB_RUN_ID']
SOURCE_BUCKET_NAME = args.get('bucket_name', None)
SOURCE_OBJECT_KEY = args.get('object_key', None)
TRACE_ID = args.get('trace_id', None)
OTLP_ENDPOINT = args.get('otlp_endpoint', None)
XRAY_HELPER_KEY = args.get('xray_helper_key', None)

# Prepare X-Ray Helper
current_dir = os.getcwd()
xray_helper_path = os.path.join(current_dir, f'{XRAY_HELPER_KEY}.py')
print(f"XRay Helper Path: {xray_helper_path}")
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

## Processing Data-set
print("Starting Glue Job Execution")
with tracer.start_as_current_span("Glue Job Execution", context=context, kind=trace.SpanKind.SERVER, attributes={'job_name': JOB_NAME, 'job_run_id': JOB_RUN_ID}):
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Read data directly from S3
    s3_path = f"s3a://{SOURCE_BUCKET_NAME}/{SOURCE_OBJECT_KEY}"
    print(f"Reading data from S3: {s3_path}")
    with tracer.start_as_current_span("Read Data", attributes={'S3Path': s3_path}):
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
        print("Data schema:")
        df.printSchema()

    # Data Processing Functions
    def drop_columns(df, col_list):
        print(f"Dropping columns: {col_list}")
        with tracer.start_as_current_span("Drop Columns"):
            for x in col_list:
                df = df.drop(x)
        return df

    def con_curr(df, col_nm):
        print(f"Converting currency for column: {col_nm}")
        with tracer.start_as_current_span("Convert Currency", attributes={'column': col_nm}):
            df = df.withColumn(col_nm, trim(regexp_replace(col_nm,'[\\$ ,]', '')).cast('float'))
            df = df.filter(df[col_nm].isNotNull())
        return df

    def fix_skew(df, col_name):
        print(f"Fixing skewness for column: {col_name}")
        with tracer.start_as_current_span("Fix Skewness", attributes={'column': col_name}):
            df = df.withColumn(col_name, col(col_name).cast('float'))
            median_value = df.approxQuantile(col_name, [0.5], 0.25)[0]
            print(f"Median value for {col_name}: {median_value}")
            df = df.withColumn(col_name, when(col(col_name).isNull(), lit(median_value)).otherwise(col(col_name)))
            df = df.withColumn(col_name, sqrt(col(col_name)))
        return df

    def correct_review(df, col_nm):
        print(f"Correcting review dates for column: {col_nm}")
        with tracer.start_as_current_span("Correct Review Dates", attributes={'column': col_nm}):
            df= df.withColumn("date_formatted", to_date(col_nm, "M/d/yyyy"))
            df= df.withColumn("date_formatted_epoch", unix_timestamp("date_formatted"))
            condition = ((year("date_formatted") > 2024) |(year("date_formatted") < col("construction_year")))
            median_val = df.approxQuantile("date_formatted_epoch", [0.5], 0.25)[0]
            print(f"Median epoch value for review dates: {median_val}")
            df= df.withColumn("abc_new",when(condition, lit(median_val)).otherwise(col("date_formatted_epoch")))
            df = df.fillna(median_val, subset=["abc_new"])
            df = df.withColumn("last_review_final", to_date(from_unixtime("abc_new")))
            df = df.drop(*['last_review','date_formatted','date_formatted_epoch','abc_new'])
            df = df.withColumnRenamed("last_review_final", "last_review")
        return df

    # Data Processing
    print("Starting data processing")
    with tracer.start_as_current_span("Data Processing"):
        # Standardizing column names
        col_nms = [x.lower().replace(' ', '_') for x in df.columns]
        df = df.toDF(*col_nms)
        print("Standardized column names:", df.columns)

        # Dropping unwanted columns
        col_list = ["host_id", "country_code", "license", "availability_365"]
        df = drop_columns(df, col_list)

        # Removing duplicates
        print("Removing duplicates")
        df = df.drop_duplicates(subset=['host_name', 'lat', 'long', 'price'])

        # Filling in null values
        print("Filling null values")
        df = df.fillna('Blank', ['house_rules', 'host_name', 'name'])
        df = df.fillna('United States', ['country'])
        df = df.fillna('Unconfirmed', ['host_identity_verified'])
        df = df.fillna('TRUE', 'instant_bookable')
        df = df.fillna('moderate', 'cancellation_policy')

        # Filtering the host_ids
        print("Filtering minimum nights")
        df = df.filter((col('minimum_nights') > 0) & (col('minimum_nights') < 365))

        # Round the number of reviews
        print("Rounding number of reviews")
        df = df.withColumn("number_of_reviews", round(df["number_of_reviews"]))

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
        df = df.replace('brookln', 'Brooklyn', 'neighbourhood_group')

        # Dropping remaining null values
        print("Dropping remaining null values")
        df = df.dropna()

        # Filtering room type
        print("Filtering room type")
        df = df.filter(length(col('room_type')) > 4)

        # Fixing column datatypes
        print("Fixing column datatypes")
        df = df.withColumn("id", monotonically_increasing_id())
        df = df.withColumn("price", col("price").cast("int"))
        df = df.withColumn("service_fee", col("service_fee").cast("int"))
        df = df.withColumn("minimum_nights", col("minimum_nights").cast("int"))
        df = df.withColumn("number_of_reviews", col("number_of_reviews").cast("int"))
        df = df.withColumn("reviews_per_month", round(col("reviews_per_month").cast("int")))
        df = df.withColumn("calculated_host_listings_count", round(col("calculated_host_listings_count").cast("int")))

    # Writing processed data directly to S3
    output_s3_path = f"s3a://{SOURCE_BUCKET_NAME}/cleaned"
    print(f"Writing processed data to S3: {output_s3_path}")
    with tracer.start_as_current_span("Write Data to S3", attributes={'S3Path': output_s3_path}):
        df.write.mode("overwrite").parquet(output_s3_path)

    print("Committing Glue job")
    job.commit()

print("Glue job execution completed")