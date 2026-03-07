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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1772800423404 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1772800423404")

# Script generated for node Step_trainer Trusted
Step_trainerTrusted_node1772801938905 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="Step_trainerTrusted_node1772801938905")

# Script generated for node SQL Query
SqlQuery5195 = '''
SELECT
    s.*,
    a.timestamp
FROM step_trainer_trusted s
JOIN accelerometer_trusted a
ON s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1772805783191 = sparkSqlQuery(glueContext, query = SqlQuery5195, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1772800423404, "step_trainer_trusted":Step_trainerTrusted_node1772801938905}, transformation_ctx = "SQLQuery_node1772805783191")

# Script generated for node Machine_learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1772805783191, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772790179369", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Machine_learningCurated_node1772806034324 = glueContext.getSink(path="s3://jammal/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Machine_learningCurated_node1772806034324")
Machine_learningCurated_node1772806034324.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
Machine_learningCurated_node1772806034324.setFormat("json")
Machine_learningCurated_node1772806034324.writeFrame(SQLQuery_node1772805783191)
job.commit()