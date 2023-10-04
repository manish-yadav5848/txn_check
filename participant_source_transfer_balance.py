from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxadsumt_xxadhsdx_jb_df = spark.sql("select client_id, plan_number,participant_id,sequence_number AS transfer_balance_seq_number, cast(source_cycle_date as DATE) as source_cycle_date , 'VRP-SP' as source_system,cast(last_day(source_cycle_date) as date) as source_month_end_date,from_fund_source as from_fund_id, to_fund_source as to_fund_id, cast(allocation_percent as DECIMAL(7,4)) as allocation_percent , cast(amount as DECIMAL(14,2)) as transfer_amount , activity as activity_type, usage_code_1, usage_code_2, usage_code_3, usage_code_4, usage_code_5, cast(rev_run_date as DATE) as reversal_run_date, rev as reversal_type_flag  from exn_xxadsumt_xxadhsdx_jb")

    exn_xxadsumt_xxadhsdx_ng_df = spark.sql("select client_id, plan_number,participant_id,sequence_number AS transfer_balance_seq_number, cast(source_cycle_date as DATE) as source_cycle_date , 'VRP-PB' as source_system,cast(last_day(source_cycle_date) as date) as source_month_end_date,from_fund_source as from_fund_id, to_fund_source as to_fund_id, cast(allocation_percent as DECIMAL(7,4)) as allocation_percent , cast(amount as DECIMAL(14,2)) as transfer_amount , activity as activity_type, usage_code_1, usage_code_2, usage_code_3, usage_code_4, usage_code_5, cast(rev_run_date as DATE) as reversal_run_date, rev as reversal_type_flag  from exn_xxadsumt_xxadhsdx_ng")

    transform_df = exn_xxadsumt_xxadhsdx_jb_df.unionByName(exn_xxadsumt_xxadhsdx_ng_df)

    return transform_df