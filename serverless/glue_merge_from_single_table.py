import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "ted_2019", table_name = "1_cca_ted_extracted_dev", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "ted_2019", table_name = "1_cca_ted_extracted_dev", transformation_ctx = "datasource0")
partitioned_dataframe = datasource0.toDF().repartition(1)
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")
datasink4 = glueContext.write_dynamic_frame.from_options(frame = partitioned_dynamicframe, connection_type = "s3", connection_options = {"path": "s3://1-cca-ted-extracted-dev/merged"}, format = "parquet", transformation_ctx = "partitioned_df")

job.commit()