import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1736764922688 = glueContext.create_dynamic_frame.from_catalog(database="final_raw_data", table_name="seogwipo_bike_storage_info", transformation_ctx="AWSGlueDataCatalog_node1736764922688")

# Script generated for node Change Schema
ChangeSchema_node1736764958632 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1736764922688, mappings=[("name", "string", "name", "string"), ("addressdoro", "string", "addressdoro", "string"), ("addressjibeon", "string", "addressjibeon", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("storenumber", "long", "storenumber", "long"), ("installationyear", "long", "installationyear", "long"), ("airinjectorflag", "string", "airinjectorflag", "string"), ("repairdeskflag", "string", "repairdeskflag", "string"), ("phonenumber", "string", "phonenumber", "string"), ("managementagency", "string", "managementagency", "string"), ("registrationdate", "string", "registrationdate", "date"), ("installationform", "string", "installationform", "string"), ("awningflag", "string", "awningflag", "string"), ("airinjectortype", "string", "airinjectortype", "string")], transformation_ctx="ChangeSchema_node1736764958632")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1736764958632, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736764858861", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1736765031816 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1736764958632, connection_type="s3", format="glueparquet", connection_options={"path": "s3://airflow-test001/transformed_data/seogwipo-bike-storage-info/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1736765031816")

job.commit()