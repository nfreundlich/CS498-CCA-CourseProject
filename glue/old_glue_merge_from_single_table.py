import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import substring

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
YEAR = 2018

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "cca_ted_extracted_dev", table_name = "2_cca_ted_extracted_dev", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "cca_ted_extracted_dev", table_name = "2_cca_ted_extracted_dev", transformation_ctx = "datasource0")

# Add a column with just the year, first 4 characters of DATE
df1 = datasource0.toDF()
df2 = df1.withColumn('YEAR', df1['DATE'].substr(0, 4))
partitioned_dataframe = df2.repartition(1)
partitioned_dataframe.write.parquet("s3://2-cca-ted-extracted-dev/merged", 'append', partitionBy='YEAR')
# convert back to DynamicFrame
# ddf = DynamicFrame.fromDF(df1, glueContext, "dynamic_frame_1")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://2-cca-ted-extracted-dev/merged"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
# datasink4 = glueContext.write_dynamic_frame.from_options(frame = ddf, connection_type = "s3", connection_options = {"path": "s3://2-cca-ted-extracted-dev/merged", "partitionKeys": ["YEAR"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()