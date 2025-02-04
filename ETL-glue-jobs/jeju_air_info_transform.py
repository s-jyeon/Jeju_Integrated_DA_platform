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
AWSGlueDataCatalog_node1736386714883 = glueContext.create_dynamic_frame.from_catalog(database="final_raw_data", table_name="jeju_air_info", transformation_ctx="AWSGlueDataCatalog_node1736386714883")

# Script generated for node SQL Query
SqlQuery103 = '''
SELECT
     DATE_FORMAT(to_date(`date`, 'yyyyMMdd'), 'yyyy-MM-dd') AS `date`,  
    `site`,  -- site 값 그대로
    `pm10`['long'] AS `pm10`  -- pm10 필드의 long 값을 추출
FROM myDataSource
'''
SQLQuery_node1736386893678 = sparkSqlQuery(glueContext, query = SqlQuery103, mapping = {"myDataSource":AWSGlueDataCatalog_node1736386714883}, transformation_ctx = "SQLQuery_node1736386893678")

# Script generated for node Change Schema
ChangeSchema_node1736388362630 = ApplyMapping.apply(frame=SQLQuery_node1736386893678, mappings=[("date", "string", "date", "date"), ("site", "long", "site", "long"), ("pm10", "long", "pm10", "long")], transformation_ctx="ChangeSchema_node1736388362630")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1736388362630, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736386710433", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1736386945653 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1736388362630, connection_type="s3", format="glueparquet", connection_options={"path": "s3://airflow-test001/transformed_data/jeju-air-info/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1736386945653")

job.commit()