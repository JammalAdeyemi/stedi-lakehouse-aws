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

# Script generated for node Customer Trusted
CustomerTrusted_node1772745894463 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1772745894463")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1772745885522 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1772745885522")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1772745888814 = Join.apply(frame1=AccelerometerLanding_node1772745885522, frame2=CustomerTrusted_node1772745894463, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1772745888814")

# Script generated for node Drop Fields
SqlQuery4482 = '''
select user, x, y, z from myDataSource

'''
DropFields_node1772746304931 = sparkSqlQuery(glueContext, query = SqlQuery4482, mapping = {"myDataSource":CustomerPrivacyFilter_node1772745888814}, transformation_ctx = "DropFields_node1772746304931")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1772746304931, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772745370932", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1772745892643 = glueContext.getSink(path="s3://jammal/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1772745892643")
AccelerometerTrusted_node1772745892643.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1772745892643.setFormat("json")
AccelerometerTrusted_node1772745892643.writeFrame(DropFields_node1772746304931)
job.commit()