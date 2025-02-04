import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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
AWSGlueDataCatalog_node1736475152762 = glueContext.create_dynamic_frame.from_catalog(database="final_raw_data", table_name="jeju_free_wifi", transformation_ctx="AWSGlueDataCatalog_node1736475152762")

# Script generated for node SQL Query
SqlQuery115 = '''
select 
  date_format(to_date(`basedate`,'yyyyMMdd'), 'yyyy-MM-dd' ) as `basedate`,
  `macaddress`, 
  `apgroupname`, 
  `installlocationdetail`, 
  `category`, 
  `categorydetail`, 
  `addressdong`, 
  `addressdetail`, 
  `latitude` , 
  `longitude` 
from myDataSource;
'''
SQLQuery_node1736475168817 = sparkSqlQuery(glueContext, query = SqlQuery115, mapping = {"myDataSource":AWSGlueDataCatalog_node1736475152762}, transformation_ctx = "SQLQuery_node1736475168817")

# Script generated for node Change Schema
ChangeSchema_node1736475176376 = ApplyMapping.apply(frame=SQLQuery_node1736475168817, mappings=[("basedate", "string", "basedate", "date"), ("macaddress", "string", "macaddress", "string"), ("apgroupname", "string", "apgroupname", "string"), ("installlocationdetail", "string", "installlocationdetail", "string"), ("category", "string", "category", "string"), ("categorydetail", "string", "categorydetail", "string"), ("addressdong", "string", "addressdong", "string"), ("addressdetail", "string", "addressdetail", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double")], transformation_ctx="ChangeSchema_node1736475176376")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1736475176376, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736475147471", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1736475180785 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1736475176376, connection_type="s3", format="glueparquet", connection_options={"path": "s3://airflow-test001/transformed_data/jeju-free-wifi/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1736475180785")

job.commit()