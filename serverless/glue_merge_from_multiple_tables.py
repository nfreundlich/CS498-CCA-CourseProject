import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# read the datasources in and convert them to DFs
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "ted_2017", table_name = "20170700_all_parquet", transformation_ctx = "datasource0").toDF()
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "ted_2017", table_name = "20170800_all_parquet", transformation_ctx = "datasource1").toDF()
datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "ted_2017", table_name = "20170900_all_parquet", transformation_ctx = "datasource2").toDF()

## join the dataframes
df0 = datasource0.union(datasource1)
df1 = df0.union(datasource2)

# repartition
partitioned_dataframe = df1.repartition(1)

# put back to DDF
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# write and upload
datasink4 = glueContext.write_dynamic_frame.from_options(frame = partitioned_dynamicframe, connection_type = "s3", connection_options = {"path": "s3://1-cca-ted-extracted-dev/2017/"}, format = "parquet", transformation_ctx = "partitioned_df")
job.commit()