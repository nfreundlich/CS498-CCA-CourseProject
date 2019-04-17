from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import boto3
from pyspark.context import SparkContext
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode, lit, col

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)

# TODO Pass the region in as a parameter
glue = boto3.client('glue', 'eu-west-1')
tables = glue.get_tables(DatabaseName='cca_ted_extracted_dev')['TableList']
tables = [table['Name'] for table in tables if table['Name'].startswith('year_')]

dfs = [
    glue_context.create_dynamic_frame \
        .from_catalog('cca_ted_extracted_dev', table)
        .filter(
            lambda record: record['category'] == 'ORIGINAL' and record['td_document_type'] == 'Contract award notice'
        )
        .select_fields(
            [
                'original_cpv_text',
                'award_contract__awarded_contract__contractors__contractor__address_contractor__officialname'
            ]
        )
        .rename_field('original_cpv_text', 'service')
        .rename_field(
            'award_contract__awarded_contract__contractors__contractor__address_contractor__officialname',
            'contractor'
        )
        .filter(lambda row: all(row['service']) and all(row['contractor']) and len(row['contractor']) == 1)
        .toDF()
    for table in tables
]

combined = spark_context \
    .union([df.rdd for df in dfs]) \
    .toDF()

combined = combined \
    .select(
        explode('service'),
        combined.contractor.getItem(0)
    ) \
    .withColumnRenamed('col', 'service') \
    .withColumnRenamed('contractor[0]', 'contractor')

service_string_indexer = StringIndexer(inputCol='service', outputCol='service_id').fit(combined)
contractor_string_indexer = StringIndexer(inputCol='contractor', outputCol="contractor_id").fit(combined)

indexed = service_string_indexer.transform(combined)
indexed = contractor_string_indexer.transform(indexed)
indexed = indexed.withColumn('rating', lit(1))

als = ALS(
    maxIter=5,
    regParam=0.01,
    userCol='service_id',
    itemCol='contractor_id',
    implicitPrefs=True,
    coldStartStrategy='drop'
)

model = als.fit(indexed)

recommendations = model.recommendForAllItems(10)

recommendations = recommendations \
    .select(
        recommendations.contractor_id,
        explode('recommendations')
    ) \
    .select(
        recommendations.contractor_id,
        col('col.service_id'),
        col('col.rating')
    )

contractor_index_to_string = IndexToString(
    inputCol='contractor_id',
    outputCol='contractor',
    labels=contractor_string_indexer.labels
)

service_index_to_string = IndexToString(
    inputCol='service_id',
    outputCol='service',
    labels=service_string_indexer.labels
)

recommendations = contractor_index_to_string.transform(recommendations)
recommendations = service_index_to_string.transform(recommendations)

recommendations = recommendations.drop('contractor_id', 'service_id')

recommendations.write.parquet('s3://af-cca-ted-extracted-dev/recommendations/', mode='overwrite')