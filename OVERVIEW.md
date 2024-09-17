# Overview

The data processing for the AirBnB dataset is divided into two Glue jobs: a cleaning job and a processing job. Each job is instrumented with OpenTelemetry (OTEL) tracing to monitor performance and execution flow.

## Job 1: Data Cleaning

The first Glue job focuses on cleaning and preparing the raw AirBnB data. Here are the main steps:

1. **Read Data**: The job reads the raw CSV data from an S3 bucket.
2. **Standardize Column Names**: Column names are standardized (lowercase, spaces replaced with underscores).
3. **Drop Unwanted Columns**: Certain columns like "host_id", "country_code", "license", and "availability_365" are removed.
4. **Remove Duplicates**: Duplicate entries based on host name, latitude, longitude, and price are removed.
5. **Fill Null Values**: Null values in various columns are filled with default values.
6. **Filter Minimum Nights**: Entries with minimum nights between 0 and 365 are kept.
7. **Convert Currency Columns**: Price and service fee columns are converted to numeric values.
8. **Fix Skewness**: Skewness in certain numeric columns is addressed using square root transformation.
9. **Correct Review Dates**: Review dates are corrected and standardized.
10. **Final Data Type Conversions**: Various columns are converted to appropriate data types.
11. **Write Cleaned Data**: The cleaned data is written back to S3 as a Parquet file.

## Job 2: Data Processing

The second Glue job focuses on processing the cleaned data and deriving new insights. Here are the main steps:

1. **Read Cleaned Data**: The job reads the cleaned Parquet file from S3.
2. **Calculate Minimum Total Spend**: Computes the minimum total spend based on price, service fee, and minimum nights.
3. **Calculate Cost Per Night**: Determines the cost per night including service fees.
4. **Rank by Cost in Neighborhood**: Ranks listings by cost within each neighborhood.
5. **Rank by Cost Overall**: Ranks listings by cost across the entire dataset.
6. **Rank by Number of Reviews**: Ranks listings based on the number of reviews received.
7. **Calculate Total Properties**: Computes the total number of properties in the dataset.
8. **Create Property Rank Columns**: Creates columns for property ranks based on cost and reviews.
9. **Write Processed Data**: The final processed data is written back to S3 as a Parquet file.

## Tracing

Both jobs use OpenTelemetry for tracing, with the following key components:

- **X-Ray Integration**: AWS X-Ray is used for distributed tracing.
- **OTLP Exporter**: Traces are exported using the OTLP (OpenTelemetry Protocol) exporter.
- **Span Creation**: Each major operation creates a new span for detailed tracing.

## Correlating Glue Job Spans with X-Ray Traces

We leverage the AWS SDK for Python (boto3) to determine the right `parent-segment` within the `xray` trace. This ensures that the spans created by our Glue jobs are correctly associated with the right segments in the existing X-Ray trace. Here's how the correlation process works:

1. **Locate StepFunctions Segment**: 
   The helper searches the X-Ray trace for the segment that corresponds to the AWS StepFunctions state machine execution. This is the starting point, as it contains the overall structure of the workflow.

2. **Identify Step Subsegment**: 
   Within the StepFunctions segment, it looks for a specific subsegment that matches the name of the Glue job step. This narrows down the search to the exact part of the workflow of interest.

3. **Extract AWS SDK Request ID**: 
   Once the right subsegment is found, the helper extracts the AWS SDK request ID. This ID is unique to the specific execution of the Glue job.

4. **Retrieve AWS SDK Segment ID**: 
   Using this request ID, the helper finds the corresponding segment ID in the X-Ray trace. This segment ID represents the Glue job execution in the context of the entire distributed trace.

5. **Correlate Spans**: 
   Finally, this segment ID is used to link the OpenTelemetry spans created by the Glue job with the correct X-Ray trace segment. This is the key step that ties everything together.

By following this process, the detailed spans from the Glue jobs are correctly placed within the broader context of the distributed trace. This maintains the continuity and accuracy of the tracing, allowing for a complete picture of the data processing pipeline's performance and behavior.

This correlation process is crucial for maintaining the integrity of the distributed tracing across the entire workflow, from the StepFunctions execution down to the individual Glue job operations.