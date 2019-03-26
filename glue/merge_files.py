import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
# get the year to process
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YEAR', 'BUCKET'])
    prefix = str(args['YEAR'])
    bucket = str(args['BUCKET'])
except:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    prefix = "2019"
    bucket = "2-cca-ted-extracted-dev"

# define the mappings
mappings = [("year","string","year","string"),("aa_authority_type", "string", "aa_authority_type", "string"), ("aa_authority_type__code", "string", "aa_authority_type__code", "char"), ("ac_award_crit", "string", "ac_award_crit", "string"), ("ac_award_crit__code", "string", "ac_award_crit__code", "string"), ("category", "string", "category", "string"), ("date", "string", "date", "string"), ("ds_date_dispatch", "string", "ds_date_dispatch", "string"), ("file", "string", "file", "string"), ("heading", "string", "heading", "string"), ("iso_country__value", "string", "iso_country__value", "string"), ("lg", "string", "lg", "string"), ("lg_orig", "array", "lg_orig", "array"), ("nc_contract_nature", "string", "nc_contract_nature", "string"), ("nc_contract_nature__code", "string", "nc_contract_nature__code", "string"), ("no_doc_ojs", "string", "no_doc_ojs", "string"), ("original_cpv", "array", "original_cpv", "array"), ("original_cpv_code", "array", "original_cpv_code", "array"), ("original_cpv_text", "array", "original_cpv_text", "array"), ("original_cpv__code", "array", "original_cpv__code", "array"), ("pr_proc", "string", "pr_proc", "string"), ("pr_proc__code", "string", "pr_proc__code", "string"), ("ref_no", "string", "ref_no", "string"), ("rp_regulation", "string", "rp_regulation", "string"), ("rp_regulation__code", "string", "rp_regulation__code", "string"), ("td_document_type", "string", "td_document_type", "string"), ("td_document_type__code", "string", "td_document_type__code", "string"), ("ty_type_bid", "string", "ty_type_bid", "string"), ("ty_type_bid__code", "string", "ty_type_bid__code", "string"), ("complementary_info__address_review_body__address", "string", "complementary_info__address_review_body__address", "string"), ("complementary_info__address_review_body__country__value", "string", "complementary_info__address_review_body__country__value", "string"), ("complementary_info__address_review_body__officialname", "string", "complementary_info__address_review_body__officialname", "string"), ("complementary_info__address_review_body__postal_code", "string", "complementary_info__address_review_body__postal_code", "string"), ("complementary_info__address_review_body__town", "string", "complementary_info__address_review_body__town", "string"), ("complementary_info__date_dispatch_notice", "string", "complementary_info__date_dispatch_notice", "string"), ("contracting_body__address_contracting_body__address", "string", "contracting_body__address_contracting_body__address", "string"), ("contracting_body__address_contracting_body__country__value", "string", "contracting_body__address_contracting_body__country__value", "string"), ("contracting_body__address_contracting_body__e_mail", "string", "contracting_body__address_contracting_body__e_mail", "string"), ("contracting_body__address_contracting_body__officialname", "string", "contracting_body__address_contracting_body__officialname", "string"), ("contracting_body__address_contracting_body__postal_code", "string", "contracting_body__address_contracting_body__postal_code", "string"), ("contracting_body__address_contracting_body__town", "string", "contracting_body__address_contracting_body__town", "string"), ("contracting_body__address_contracting_body__url_general", "string", "contracting_body__address_contracting_body__url_general", "string"), ("contracting_body__address_contracting_body__n2016_nuts__code", "string", "contracting_body__address_contracting_body__n2016_nuts__code", "string"), ("form", "string", "form", "string"), ("ia_url_general", "string", "ia_url_general", "string"), ("initiator", "string", "initiator", "string"), ("ma_main_activities", "array", "ma_main_activities", "array"), ("ma_main_activities__code", "array", "ma_main_activities__code", "array"), ("object_contract__cpv_main__cpv_code__code", "string", "object_contract__cpv_main__cpv_code__code", "string"), ("object_contract__object_descr__cpv_additional__cpv_code__code", "array", "object_contract__object_descr__cpv_additional__cpv_code__code", "array"), ("object_contract__object_descr__item", "array", "object_contract__object_descr__item", "array"), ("object_contract__object_descr__short_descr", "array", "object_contract__object_descr__short_descr", "array"), ("object_contract__object_descr__n2016_nuts__code", "array", "object_contract__object_descr__n2016_nuts__code", "array"), ("object_contract__short_descr", "array", "object_contract__short_descr", "array"), ("object_contract__title", "string", "object_contract__title", "string"), ("object_contract__type_contract__ctype", "string", "object_contract__type_contract__ctype", "string"), ("n2016_ca_ce_nuts", "array", "n2016_ca_ce_nuts", "array"), ("n2016_ca_ce_nuts__code", "array", "n2016_ca_ce_nutsca_ce_nuts__code", "array"), ("n2016_performance_nuts", "array", "n2016_performance_nuts", "array"), ("n2016_performance_nuts__code", "array", "n2016_performance_nuts__code", "array"), ("complementary_info__address_review_body__phone", "string", "complementary_info__address_review_body__phone", "string"), ("contracting_body__address_contracting_body__phone", "string", "contracting_body__address_contracting_body__phone", "string"), ("contracting_body__address_contracting_body__url_buyer", "string", "contracting_body__address_contracting_body__url_buyer", "string"), ("contracting_body__ca_activity__value", "string", "contracting_body__ca_activity__value", "string"), ("contracting_body__ca_type__value", "string", "contracting_body__ca_type__value", "string"), ("legal_basis__value", "string", "legal_basis__value", "string"), ("object_contract__reference_number", "string", "object_contract__reference_number", "string"), ("ref_notice__no_doc_ojs", "string", "ref_notice__no_doc_ojs", "string"), ("values__value", "string", "values__value", "float"), ("value_eur", "string", "value_eur", "float"), ("values__value__currency", "string", "values__value__currency", "string"), ("values__value__type", "string", "values__value__type", "string"), ("award_contract__item", "array", "award_contract__item", "array"), ("award_contract__awarded_contract__date_conclusion_contract", "array", "award_contract__awarded_contract__date_conclusion_contract", "array"), ("award_contract__title", "array", "award_contract__title", "array"), ("object_contract__val_total", "string", "object_contract__val_total", "float"), ("object_contract__val_total__currency", "string", "object_contract__val_total__currency", "string"), ("procedure__notice_number_oj", "string", "procedure__notice_number_oj", "string"), ("n2016_tenderer_nuts", "array", "n2016_tenderer_nuts", "array"), ("n2016_tenderer_nuts__code", "array", "n2016_tenderer_nuts__code", "array"), ("contracting_body__url_document", "string", "contracting_body__url_document", "string"), ("contracting_body__url_participation", "string", "contracting_body__url_participation", "string"), ("dt_date_for_submission", "string", "dt_date_for_submission", "string"), ("ia_url_etendering", "string", "ia_url_etendering", "string"), ("object_contract__object_descr__duration", "array", "object_contract__object_descr__duration", "array"), ("object_contract__object_descr__duration__type", "array", "object_contract__object_descr__duration__type", "array"), ("procedure__date_receipt_tenders", "string", "procedure__date_receipt_tenders", "date"), ("procedure__languages__language__value", "array", "procedure__languages__language__value", "array"), ("procedure__opening_condition__date_opening_tenders", "string", "procedure__opening_condition__date_opening_tenders", "date"), ("procedure__opening_condition__time_opening_tenders", "string", "procedure__opening_condition__time_opening_tenders", "string"), ("procedure__time_receipt_tenders", "string", "procedure__time_receipt_tenders", "string"), ("award_contract__awarded_contract__contractors__contractor__address_contractor__country__value", "array", "award_contract__awarded_contract__contractors__contractor__address_contractor__country__value", "array"), ("award_contract__awarded_contract__contractors__contractor__address_contractor__officialname", "array", "award_contract__awarded_contract__contractors__contractor__address_contractor__officialname", "array"), ("award_contract__awarded_contract__contractors__contractor__address_contractor__postal_code", "array", "award_contract__awarded_contract__contractors__contractor__address_contractor__postal_code", "array"), ("award_contract__awarded_contract__contractors__contractor__address_contractor__town", "array", "award_contract__awarded_contract__contractors__contractor__address_contractor__town", "array"), ("award_contract__awarded_contract__contractors__contractor__address_contractor__n2016_nuts__code", "array", "award_contract__awarded_contract__contractors__contractor__address_contractor__n2016_nuts__code", "array"), ("award_contract__awarded_contract__tenders__nb_tenders_received", "array", "award_contract__awarded_contract__tenders__nb_tenders_received", "array"), ("award_contract__awarded_contract__values__val_total", "int", "award_contract__awarded_contract__values__val_total", "int"), ("award_contract__awarded_contract__values__val_total__currency", "int", "award_contract__awarded_contract__values__val_total__currency", "int"), ("award_contract__contract_no", "array", "award_contract__contract_no", "array"), ("award_contract__lot_no", "array", "award_contract__lot_no", "array"), ("complementary_info__address_review_body__e_mail", "string", "complementary_info__address_review_body__e_mail", "string"), ("complementary_info__address_review_body__fax", "string", "complementary_info__address_review_body__fax", "string"), ("complementary_info__address_review_info__country__value", "string", "complementary_info__address_review_info__country__value", "string"), ("complementary_info__address_review_info__officialname", "string", "complementary_info__address_review_info__officialname", "string"), ("complementary_info__address_review_info__town", "string", "complementary_info__address_review_info__town", "string"), ("complementary_info__info_add", "array", "complementary_info__info_add", "array"), ("lefti__suitability", "array", "lefti__suitability", "array"), ("procedure__duration_tender_valid", "string", "procedure__duration_tender_valid", "int"), ("procedure__duration_tender_valid__type", "string", "procedure__duration_tender_valid__type", "string"), ("fd_oth_not__obj_not__blk_btx", "array", "fd_oth_not__obj_not__blk_btx", "array"), ("fd_oth_not__obj_not__cpv__cpv_main__cpv_code__code", "string", "fd_oth_not__obj_not__cpv__cpv_main__cpv_code__code", "string"), ("fd_oth_not__obj_not__int_obj_not", "string", "fd_oth_not__obj_not__int_obj_not", "string"), ("fd_oth_not__sti_doc__p__address_not_struct__address", "string", "fd_oth_not__sti_doc__p__address_not_struct__address", "string"), ("fd_oth_not__sti_doc__p__address_not_struct__blk_btx", "array", "fd_oth_not__sti_doc__p__address_not_struct__blk_btx", "array"), ("fd_oth_not__sti_doc__p__address_not_struct__country__value", "string", "fd_oth_not__sti_doc__p__address_not_struct__country__value", "string"), ("fd_oth_not__sti_doc__p__address_not_struct__e_mail", "string", "fd_oth_not__sti_doc__p__address_not_struct__e_mail", "string"), ("fd_oth_not__sti_doc__p__address_not_struct__organisation", "string", "fd_oth_not__sti_doc__p__address_not_struct__organisation", "string"), ("fd_oth_not__sti_doc__p__address_not_struct__phone", "string", "fd_oth_not__sti_doc__p__address_not_struct__phone", "string"), ("fd_oth_not__sti_doc__p__address_not_struct__postal_code", "string", "fd_oth_not__sti_doc__p__address_not_struct__postal_code", "string"), ("fd_oth_not__sti_doc__p__address_not_struct__town", "string", "fd_oth_not__sti_doc__p__address_not_struct__town", "string"), ("fd_oth_not__ti_doc", "array", "fd_oth_not__ti_doc", "array"), ("main_cpv_code", "string", "main_cpv_code", "string"),("main_n2016_tenderer_nuts__code", "string", "main_n2016_tenderer_nuts__code", "string"),("main_n2016_performance_nuts__code", "string", "main_n2016_performance_nuts__code", "string"),("main_ma_main_activities__code", "string", "main_ma_main_activities__code", "string"),("main_object_contract__object_descr__duration", "string", "main_object_contract__object_descr__duration", "string"),("main_award_contract__awarded_contract__contractors__contractor__address_contractor__country__value", "string", "main_award_contract__awarded_contract__contractors__contractor__address_contractor__country__value", "string"),("version", "string", "version", "string"), ("__index_level_0__", "long", "__index_level_0__", "long")]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "cca_ted_2019", table_name = "2_cca_ted_extracted_dev", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": ["s3://" + bucket + "/" + prefix]}, format = "parquet")

## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = datasource0, transformation_ctx = "dropnullfields3")

## @type: ApplyMapping
## @args: [mapping = mappings
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = dropnullfields3 , mappings = mappings, transformation_ctx = "applymapping1")

## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")

# partition the DataFrame to 1 partition
partitioned_dataframe = resolvechoice2.toDF().repartition(1)

# convert back to DynamicFrame
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://2-cca-ted-extracted-dev/merged"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = partitioned_dynamicframe, connection_type = "s3", connection_options = {"path": "s3://" + bucket + "/merged", "partitionKeys": ["YEAR"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
