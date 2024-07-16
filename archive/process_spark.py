import sys
import os
import importlib.util
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, concat, lit, count

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
JOB_RUN_ID = args['JOB_RUN_ID']
SOURCE_BUCKET_NAME = args.get('bucket_name', None)
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

print("Setting up instrumentation")
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
with tracer.start_as_current_span("Glue Job Execution", context=context, kind=trace.SpanKind.SERVER, attributes={'job_name': JOB_NAME, 'job_run_id': JOB_RUN_ID}):
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    # Read data
    print("Reading input data")
    with tracer.start_as_current_span("Read from S3"):
        input_path = f's3://{SOURCE_BUCKET_NAME}/cleaned'
        spark_df = spark.read.parquet(input_path)
    print(f"Created Spark DataFrame with {spark_df.count()} rows")

    # Data Processing
    print("Starting data processing")
    with tracer.start_as_current_span("Data Processing"):
        with tracer.start_as_current_span("Calculate Minimum Total Spend"):
            print("Calculating minimum total spend")
            spark_df = spark_df.withColumn("minimum_total_spend", (col("price") + col("service_fee")) * col('minimum_nights'))
        
        with tracer.start_as_current_span("Calculate Cost Per Night"):
            print("Calculating cost per night")
            spark_df = spark_df.withColumn("cost_per_night", (col("price") + col("service_fee")))
        
        with tracer.start_as_current_span("Rank by Cost Per Night in Neighborhood"):
            print("Ranking by cost per night in neighborhood")
            window = Window.partitionBy("neighbourhood").orderBy(col("cost_per_night").desc())
            spark_df = spark_df.withColumn("exp_rank_per_neighbourhood", rank().over(window))
        
        with tracer.start_as_current_span("Rank by Cost Per Night Overall"):
            print("Ranking by cost per night overall")
            window = Window.orderBy(col("cost_per_night").desc())
            spark_df = spark_df.withColumn("exp_rank_overall", rank().over(window))
        
        with tracer.start_as_current_span("Rank by Number of Reviews"):
            print("Ranking by number of reviews")
            window = Window.orderBy(col("number_of_reviews").desc())
            spark_df = spark_df.withColumn("rank_overall_reviews", rank().over(window))
        
        with tracer.start_as_current_span("Calculate Total Properties"):
            print("Calculating total number of properties")
            total_no_props = str(int(spark_df.describe("exp_rank_overall").filter("summary = 'max'").select("exp_rank_overall").first().asDict()['exp_rank_overall']))
            print(f"Total number of properties: {total_no_props}")
        
        with tracer.start_as_current_span("Create Property Rank Columns"):
            print("Creating property rank columns")
            spark_df = spark_df.withColumn("prop_rank_exp", concat(col("exp_rank_overall").cast('string'), lit('/'), lit(total_no_props)))
            spark_df = spark_df.withColumn("prop_rank_review", concat(col("rank_overall_reviews").cast('string'), lit('/'), lit(total_no_props)))

    # Write processed data
    print("Writing processed data")
    with tracer.start_as_current_span("Write to S3"):
        output_path = f's3://{SOURCE_BUCKET_NAME}/processed'
        spark_df.write.parquet(output_path, mode="overwrite")

    print("Committing Glue job")
    job.commit()

print("Glue job execution completed")