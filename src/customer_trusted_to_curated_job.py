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
CustomerTrusted_node1772747647586 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1772747647586")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1772747659540 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1772747659540")

# Script generated for node Customer Columns Only
SqlQuery2590 = '''
select 
    first(customerName) as customerName,
    email,
    first(phone) as phone,
    first(birthDay) as birthDay,
    first(serialNumber) as serialNumber,
    first(registrationDate) as registrationDate,
    first(lastUpdateDate) as lastUpdateDate,
    first(shareWithResearchAsOfDate) as shareWithResearchAsOfDate,
    first(shareWithPublicAsOfDate) as shareWithPublicAsOfDate,
    first(shareWithFriendsAsOfDate) as shareWithFriendsAsOfDate
from customer_trusted ct
join accelerometer_trusted t1 
on ct.email = t1.user
group by ct.email
'''
CustomerColumnsOnly_node1772747654590 = sparkSqlQuery(glueContext, query = SqlQuery2590, mapping = {"customer_trusted":CustomerTrusted_node1772747647586, "accelerometer_trusted":AccelerometerTrusted_node1772747659540}, transformation_ctx = "CustomerColumnsOnly_node1772747654590")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=CustomerColumnsOnly_node1772747654590, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772745370932", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1772747657170 = glueContext.getSink(path="s3://jammal/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1772747657170")
CustomerCurated_node1772747657170.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customers_curated")
CustomerCurated_node1772747657170.setFormat("json")
CustomerCurated_node1772747657170.writeFrame(CustomerColumnsOnly_node1772747654590)
job.commit()