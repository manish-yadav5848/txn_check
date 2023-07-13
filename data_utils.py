__author__ = "lumiq"

from pyspark.sql import SparkSession
from wealthcentral.jobs.staging_to_raw.helper_utils.objects_utils import FileParameters, TableNameToSchemaMapper
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType
from pyspark.sql.window import Window
from wealthcentral.utils.object_utils import StagingJobArguments


# from datetime import time, datetime, timedelta


def get_file_segments_in_dir(dbutil, job_arguments: StagingJobArguments):
    dir_path = f"{os.getenv('STAGING_INPUT_PATH')}/{job_arguments.batch_date}/{job_arguments.source_name}/{job_arguments.file_name}"
    files = dbutil.fs.ls(dir_path)
    file_segments = [file.name for file in files]

    return file_segments


def read_data_from_file(spark: SparkSession, job_arguments: StagingJobArguments, file_parameters: FileParameters):
    """
    For a source type and source config, it reads the data and
    returns it in the form of spark dataframe
    :param spark: spark session
    :param source_type: string signifying the source type
    :param source_config: dictionary with all the necessary information about a source data
    :return: spark dataframe
    """
    df = spark.createDataFrame([], StructType([]))
    if file_parameters.file_type == 'delimited':
        df = read_delimited_file(
            spark=spark,
            job_arguments=job_arguments,
            file_parameters=file_parameters
        )
    elif file_parameters.file_type == 'fixed_width':
        df = read_fixed_width_file(
            spark=spark,
            job_arguments=job_arguments,
            file_parameters=file_parameters
        )
    return df


def read_delimited_file(spark: SparkSession, job_arguments: StagingJobArguments, file_parameters: FileParameters):
    delimiter_mapper = {
        "psv": "|",
        "csv": ","
    }
    if file_parameters.file_header_flag:
        options = {
            "header": "true",
            "inferSchema": "false",
            "delimiter": delimiter_mapper.get(file_parameters.file_format)
        }
    else:
        options = {
            "header": "false",
            "inferSchema": "false",
            "delimiter": delimiter_mapper.get(file_parameters.file_format)
        }

    schema = TableNameToSchemaMapper.get(file_parameters.table_name)

    source_load_df = spark.read \
        .options(**options) \
        .schema(schema) \
        .csv(
        f"{os.getenv('STAGING_INPUT_PATH')}/{job_arguments.batch_date}/{job_arguments.source_name}/{job_arguments.file_name}/{job_arguments.file_segment_name}*")

    # Convert column names to lowercase and trim
    source_load_df = source_load_df.select([trim(col(c)).alias(c.lower()) for c in source_load_df.columns])

    return source_load_df


def read_fixed_width_file(spark: SparkSession, job_arguments: StagingJobArguments, file_parameters: FileParameters):
    options = {
        "header": "false",
        "inferSchema": "false",
        "encoding": 'UTF-16'
    }
    schema = TableNameToSchemaMapper.get(file_parameters.table_name)

    source_load_df = spark.read \
        .options(**options) \
        .text(
        f"{os.getenv('STAGING_INPUT_PATH')}/{job_arguments.batch_date}/{job_arguments.source_name}/{job_arguments.file_name}/{job_arguments.file_segment_name}*")

    # Handle null characters and extra characters in df
    null_char = u'\x00\x00'
    source_load_df = source_load_df.select(
        *(regexp_replace(col(c), null_char, ' ').alias(c) for c in source_load_df.columns))
    source_load_df = source_load_df.filter(~ col('value').startswith("-- "))

    for column in schema:
        column_info = schema[column]
        start_pos = column_info["start"]
        end_pos = column_info["end"]
        column_type = column_info["type"]
        column_nullable = column_info["nullable"]

        source_load_df = source_load_df \
            .withColumn(column,
                        trim(substring(source_load_df["value"], start_pos, end_pos - start_pos + 1).cast(column_type)))

    # Convert Columns to lowercase
    source_load_df = source_load_df.select([col(c).alias(c.lower()) for c in source_load_df.columns])
    source_load_df = source_load_df.drop('value')

    return source_load_df


def add_col_file_segment(df, value):
    return df.withColumn("file_segment", lit(value))


def remove_junk_values_from_cols(df):
    # Remove '' and \r\n from columns
    for column_name in df.columns:
        df = df \
            .withColumn(column_name, when(col(column_name) == '', lit(None)).otherwise(col(column_name))) \
            .withColumn(column_name, regexp_replace(col(column_name), "[\n\r]", " "))

    return df


def fill_source_cycle_date_where_nil(df, batch_date):
    cycle_date_column_list = ['dss_cycle_date', 'source_cycle_date', 'cycle_date', 'prx_cycle_date', 'run_date',
                              'valuation_date']

    for cycle_date_column in cycle_date_column_list:
        if cycle_date_column in df.columns:
            df = df.withColumn(cycle_date_column, coalesce(col(cycle_date_column),
                                                           to_date(lit(batch_date), 'yyyyMMdd').cast(StringType())))

    return df


def convert_invalid_datetime_to_nil(df, date_column):
    invalid_datetime_list = [
        '0000-00-00', '0000.00.00', '0000/00/00',
        '00-00-0000', '00.00.0000', '00/00/0000',
        '00-00-00', '00.00.00', '00/00/00',
        '0000-00-00 00:00:00', '0000-00-00T00:00:00.000-0000', '0000-00-00T00:00:00.000-00:00',
        '00000000'
    ]

    df = df.withColumn(
        date_column,
        when(
            (length(col(date_column)) < 8)
            , lit(None)
        )
        .when(
            col(date_column).isin(invalid_datetime_list)
            , lit(None)
        )
        .otherwise(
            col(date_column)
        )
    )

    return df


def convert_datetime_to_single_format(df, date_column):
    df = df \
        .withColumn("d1", to_date(col(date_column), 'yyyy-MM-dd')) \
        .withColumn("d2", to_date(col(date_column), 'yyyy.MM.dd')) \
        .withColumn("d3", to_date(col(date_column), 'yyyy/MM/dd')) \
        .withColumn("d4", to_date(col(date_column), 'MM-dd-yyyy')) \
        .withColumn("d5", to_date(col(date_column), 'MM.dd.yyyy')) \
        .withColumn("d6", to_date(col(date_column), 'MM/dd/yyyy')) \
        .withColumn("d7", to_date(col(date_column), 'MM-dd-yy')) \
        .withColumn("d8", to_date(col(date_column), 'MM.dd.yy')) \
        .withColumn("d9", to_date(col(date_column), 'MM/dd/yy')) \
        .withColumn("d10", to_date(col(date_column), 'dd-MM-yyyy')) \
        .withColumn("d11", to_date(col(date_column), 'dd.MM.yyyy')) \
        .withColumn("d12", to_date(col(date_column), 'dd/MM/yyyy')) \
        .withColumn("d13", to_date(col(date_column), 'dd-MM-yy')) \
        .withColumn("d14", to_date(col(date_column), 'dd.MM.yy')) \
        .withColumn("d15", to_date(col(date_column), 'dd/MM/yy')) \
        .withColumn("d16", to_date(col(date_column), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn("d17", to_date(col(date_column), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
        .withColumn("d18", to_date(col(date_column), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
        .withColumn("d19", to_date(col(date_column), 'yyyyMMdd')) \
        .drop(date_column) \
        .withColumn(date_column,
                    coalesce("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10", "d11", "d12", "d13", "d14",
                             "d15", "d16", "d17", "d18", "d19").cast(StringType())) \
        .drop("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10", "d11", "d12", "d13", "d14", "d15", "d16",
              "d17", "d18", "d19")

    return df


def convert_date_cols_to_valid_format(df):
    # Handle Multiple Date Formats
    for date_column in df.columns:
        if (("date" in date_column) or ("_dt" in date_column) or ("_dob" in date_column)) and (
                date_column != 'batch_date'):
            df = convert_invalid_datetime_to_nil(df, date_column)
            df = convert_datetime_to_single_format(df, date_column)

    return df


# def add_col_batch_date(df, file_recv_date, file_recv_ts):
#     # If column present in batch_date_column_list
#     #      file_recv_date = batch_date_column and file_recv_ts between (14:59) and (23:59) then batch_date_column
#     #      file_recv_date = batch_date_column and file_recv_ts between (00:00) and (14:58) then batch_date_column - 1
#     #      file_recv_date > batch_date_column and then batch_date_column
#     # If column not present in batch_date_column_list
#     #      file_recv_ts between (14:59) and (23:59) then file_recv_date
#     #      file_recv_ts between (00:00) and (14:58) then file_recv_date - 1
#
#     batch_date_col_found = False
#     batch_date_column_list = ['dss_cycle_date', 'source_cycle_date', 'cycle_date', 'prx_cycle_date', 'run_date', 'valuation_date', 'as_of_date']
#
#     for batch_date_column in batch_date_column_list:
#         if batch_date_column in df.columns:
#             # Drop records where invalid batch_date_column
#             df = df.filter(
#                 (length(col(batch_date_column)) == 10)
#             )
#
#             # Add file_recv_ts to the Dataframe for below code snippet. To be dropped later
#             df = df.withColumn("file_recv_ts", lit(file_recv_ts))
#
#             # Batch Date Calculation
#             df = df.withColumn("batch_date",
#                                when(
#                                    date_format(to_date(col(batch_date_column), "yyyy-MM-dd"), "yyyyMMdd") == to_date(lit(file_recv_date),
#                                                                                           "yyyyMMdd"),
#                                    when((col('file_recv_ts') >= '14:59') & (
#                                            col('file_recv_ts') <= '23:59'),
#                                         date_format(to_date(col(batch_date_column), "yyyy-MM-dd"), "yyyyMMdd").cast(
#                                             StringType())
#                                         ).when((col('file_recv_ts') >= '00:00') & (
#                                            col('file_recv_ts') <= '14:58'),
#                                                date_format(
#                                                    date_sub(
#                                                        to_date(col(batch_date_column), "yyyy-MM-dd"), 1),
#                                                    "yyyyMMdd").cast(StringType())
#                                                )
#                                ).when(
#                                    date_format(to_date(col(batch_date_column), "yyyy-MM-dd"), "yyyyMMdd") < to_date(lit(file_recv_date),
#                                                                                          "yyyyMMdd"),
#                                    date_format(to_date(col(batch_date_column), "yyyy-MM-dd"), "yyyyMMdd").cast(
#                                        StringType())
#                                )
#                                .when(
#                                     (col('file_recv_ts') >= '14:59') & (
#                                        col('file_recv_ts') <= '23:59')
#                                     , date_format(to_date(lit(file_recv_date), "yyyyMMdd"), "yyyyMMdd").cast(StringType())
#                                 )
#                                .when(
#                                     (col('file_recv_ts') >= '00:00') & (
#                                        col('file_recv_ts') <= '14:58')
#                                     , date_format(date_sub(to_date(lit(file_recv_date), "yyyyMMdd"), 1), "yyyyMMdd").cast(
#                                    StringType())
#                                 ).otherwise(
#                                     lit(None)
#                                 )
#                                )
#             df = df.drop('file_recv_ts')
#             batch_date_col_found = True
#             break
#
#     if not batch_date_col_found:
#         df = df.withColumn('file_recv_ts', lit(file_recv_ts))
#         df = df.withColumn("batch_date",
#                            when(
#                                (col('file_recv_ts') >= '14:59') & (
#                                        col('file_recv_ts') <= '23:59')
#                                , date_format(to_date(lit(file_recv_date), "yyyyMMdd"), "yyyyMMdd").cast(StringType())
#                            ).when(
#                                (col('file_recv_ts') >= '00:00') & (
#                                        col('file_recv_ts') <= '14:58')
#                                , date_format(date_sub(to_date(lit(file_recv_date), "yyyyMMdd"), 1), "yyyyMMdd").cast(
#                                    StringType())
#                            ).otherwise(
#                                lit(None)
#                            )
#                            )
#     df = df.drop('file_recv_ts')
#     return df


def add_col_batch_date(df, batch_date: str):
    df = df.withColumn("batch_date", lit(batch_date))
    return df


# def get_distinct_batch_date(df):
#     return df.select(col("batch_date").cast(StringType())).distinct().rdd.map(lambda row: row[0]).collect()


# def create_distinct_batch_date(job_arguments: StagingJobArguments):
#     file_recv_date = job_arguments.batch_date
#     file_recv_ts = job_arguments.file_recv_ts
#     hour = int(file_recv_ts[:2])
#     minute = int(file_recv_ts[3:])
#     time_object = time(hour=hour, minute=minute)
#     formatted_file_recv_ts = time_object.strftime('%H:%M')
#
#     if formatted_file_recv_ts >= time(hour=14, minute=59).strftime('%H:%M') and formatted_file_recv_ts <= time(hour=23,
#                                                                                                                minute=59).strftime(
#         '%H:%M'):
#         batch_date_object = file_recv_date
#
#     if formatted_file_recv_ts >= time(hour=0, minute=0).strftime('%H:%M') and formatted_file_recv_ts <= time(hour=14,
#                                                                                                              minute=58).strftime(
#         '%H:%M'):
#         batch_date_object = (datetime.strptime(file_recv_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
#
#     batch_date = [batch_date_object]
#
#     return batch_date


def get_record_count(df):
    return df.count()


def generate_merge_condition_for_delta_in_staging(merge_keys, target_alias_keyword="target",
                                                  source_alias_keyword="source"):
    # target is delta table and source is load DF
    merge_condition = ' and '.join(
        '{}.{} = {}.{}'.format(target_alias_keyword, pkColumn, source_alias_keyword, pkColumn) for pkColumn in
        merge_keys)
    return merge_condition


def generate_update_condition_for_delta_in_staging(col_list, target_alias_keyword="target",
                                                   source_alias_keyword="source"):
    # target is delta table and source is load DF
    dict = {}
    for col_name in col_list:
        dict[col_name] = f"{source_alias_keyword}.{col_name}"
    return dict


def generate_insert_condition_for_delta_in_staging(col_list, target_alias_keyword="target",
                                                   source_alias_keyword="source"):
    # target is delta table and source is load DF
    dict = {}
    for col_name in col_list:
        dict[col_name] = f"{source_alias_keyword}.{col_name}"
    return dict


def add_unique_key(df, partition_key, order_key):
    # Define a window specification for the row number
    window_spec = Window.partitionBy(partition_key).orderBy(col(order_key))
    # Add the row number with prefix to the DataFrame
    df_with_unique_key = df \
        .withColumn("row_number", row_number().over(window_spec)) \
        .withColumn("newr_nqtxn_key",
                    concat(col("batch_date"), lit("_"), col("row_number").cast("string")).cast("string")) \
        .drop("row_number")

    return df_with_unique_key


def identify_duplicates(df, partition_key):
    """
    Drops duplicates over the specified partition key and keeps only the first occurrence of each duplicate.
    Parameters:
        df (DataFrame): The DataFrame to drop duplicates from.
        partition_key (list): A list of column names to partition by.
    Returns:
        DataFrame: The input DataFrame with duplicates over the partition key dropped.
    """
    # Check if source_cycle_date column available. Else use etl_run_date column
    order_key = 'batch_date'

    # Define the window specification
    window_spec = Window.partitionBy(partition_key).orderBy(order_key)

    # Add a row number to each partition
    df = df.withColumn("row_number", row_number().over(window_spec))

    # Flag the first occurrence of each duplicate for dropping
    df = df \
        .withColumn("to_drop", when(col("row_number") == 1, lit(False)).otherwise(lit(True))) \
        .withColumn("drop_record_remarks", when(col("row_number") == 1, lit(None)).otherwise(lit("duplicate record"))) \
        .drop('row_number')

    return df


def identify_invalid_sma_fund_balance(df):
    sma_fund_balance_column_name = 'sma_fund_balance'

    if sma_fund_balance_column_name in df.columns:
        df = df \
            .withColumn("to_drop", when(col(sma_fund_balance_column_name).contains(' '),
                                        coalesce(lit(True), col("to_drop"))).otherwise(col("to_drop"))) \
            .withColumn("drop_record_remarks",
                        when(col(sma_fund_balance_column_name).contains(' '),
                             coalesce(lit("invalid sma fund balance"), col("drop_record_remarks"))).otherwise(
                            col("drop_record_remarks")))

    return df


def identify_invalid_plan_number(df):
    invalid_plan_id_lb = '000001'
    invalid_plan_id_ub = '000040'

    if 'plan_id' in df.columns:
        plan_column_name = 'plan_id'
    elif 'plan_number' in df.columns:
        plan_column_name = 'plan_number'
    else:
        plan_column_name = None

    if plan_column_name is not None:
        df = df \
            .withColumn("to_drop", when(col(plan_column_name).between(invalid_plan_id_lb, invalid_plan_id_ub),
                                        coalesce(lit(True), col("to_drop"))).otherwise(col("to_drop"))) \
            .withColumn("drop_record_remarks",
                        when(col(plan_column_name).between(invalid_plan_id_lb, invalid_plan_id_ub),
                             coalesce(lit("invalid plan id"), col("drop_record_remarks"))).otherwise(
                            col("drop_record_remarks")))
    if 'client_id' in df.columns and plan_column_name is not None:
        df = df \
            .withColumn("to_drop",
                           when((col("client_id") == "NG") & (col(plan_column_name).isin('999999', '999998', '999997')),
                                coalesce(lit(True), col("to_drop"))).otherwise(col("to_drop"))) \
            .withColumn("drop_record_remarks",
                        when((col("client_id") == "NG") & (col(plan_column_name).isin('999999', '999998', '999997')),
                             coalesce(lit("invalid plan id"), col("drop_record_remarks"))).otherwise(col("drop_record_remarks")))
    return df


# Update dataframe containing transactions data by replacing representative characters with respective digits in Amount fields
def handle_amounts(df, column_name, precision, scale):
    diff = precision - scale
    df = df.withColumn(column_name, concat(substring(column_name, 1, diff), lit('.'),
                                           substring(column_name, diff + 1, precision)))

    df = df.withColumn(column_name,
                       when(
                           col(column_name).endswith('p'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'p', '0')
                           )
                       )
                       .when(
                           col(column_name).endswith('q'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'q', '1')
                           )
                       )
                       .when(
                           col(column_name).endswith('r'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'r', '2')
                           )
                       )
                       .when(
                           col(column_name).endswith('s'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 's', '3')
                           )
                       )
                       .when(
                           col(column_name).endswith('t'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 't', '4')
                           )
                       )
                       .when(
                           col(column_name).endswith('u'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'u', '5')
                           )
                       )
                       .when(
                           col(column_name).endswith('v'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'v', '6')
                           )
                       )
                       .when(
                           col(column_name).endswith('w'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'w', '7')
                           )
                       )
                       .when(
                           col(column_name).endswith('x'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'x', '8')
                           )
                       )
                       .when(
                           col(column_name).endswith('y'),
                           concat(
                               lit('-'),
                               regexp_replace(col(column_name), 'y', '9')
                           )
                       ).otherwise(
                           col(column_name)
                       )
                       )

    return df


# Replace empty rev_code values with default values
def replace_null(df):
    df = df.fillna('0', subset=['rev_code'])

    df = df.withColumn(
        'rev_code',
        when(
            col('rev_code') == '',
            '0'
        )
        .otherwise(
            col('rev_code')
        )
    )

    return df


def parse_txn_data(df):
    amounts_dict = {
        'cash': [13, 2],
        'shares': [13, 4],
        'uninvested_cash': [13, 2],
        'share_cost': [13, 2],
        'other_cash': [13, 2],
        'share_price': [13, 6],
        'currency_value': [13, 2],
        'xr_rate': [11, 6],
        'net_currency_value': [13, 2]
    }

    for key, val in amounts_dict.items():
        df = handle_amounts(df, key, val[0], val[1])

    df = replace_null(df)

    return df


# Replace empty client_id values with default values
def replace_client(df):
    df = df.fillna(value='NG', subset=['client_id'])
    df = df.withColumn(
        'client_id',
        when(
            col('client_id') == '',
            'NG'
        )
        .otherwise(
            col('client_id')
        )
    )

    return df


# Replace empty loan_type values with default values
def replace_loan_type(df):
    df = df.fillna(value='G', subset=['loan_type_id'])

    df = df.withColumn(
        'loan_type_id',
        when(
            col('loan_type_id') == '',
            'G'
        )
        .otherwise(
            col('loan_type_id')
        )
    )

    return df


def change_fund_code(df):
    df = df.withColumn(
        'fund_code',
        when(
            df.fund_iv == '90',
            'LN90'
        )
        .when(
            df.fund_iv == '91',
            'LN91'
        )
        .otherwise(
            df.fund_code
        )
    )

    return df


def change_price_id(df):
    df = df.withColumn(
        'price_id',
        when(
            df.fund_iv == '90',
            'LN90'
        )
        .when(
            df.fund_iv == '91',
            'LN91'
        )
        .otherwise(
            df.price_id
        )
    )

    return df


def filter_rejected_df(df):
    return df \
        .filter(col("to_drop") == True)


def filter_valid_df(df):
    return df \
        .filter(col("to_drop") == False) \
        .drop("to_drop", "drop_record_remarks")


def generate_merge_condition_for_delta_in_raw(merge_keys, target_alias_keyword="target", source_alias_keyword="source"):
    # target is delta table and source is load DF
    merge_condition = ' and '.join(
        '{}.{} = {}.{}'.format(target_alias_keyword, pkColumn, source_alias_keyword, pkColumn) for pkColumn in
        merge_keys)
    return merge_condition


def generate_update_condition_for_delta_in_raw(col_list, target_alias_keyword="target", source_alias_keyword="source"):
    """
    Creates a target dictionary with the specified structure.

    Parameters:
        col_list (list): A list of column names.

    Returns:
        dict: A dictionary with the specified structure.
    """
    # target is delta table and source is load DF
    dict = {}
    for col_name in col_list:
        dict[col_name] = f"{source_alias_keyword}.{col_name}"
    return dict


def generate_insert_condition_for_delta_in_raw(col_list, target_alias_keyword="target", source_alias_keyword="source"):
    """
    Creates a target dictionary with the specified structure.

    Parameters:
        col_list (list): A list of column names.

    Returns:
        dict: A dictionary with the specified structure.
    """
    # target is delta table and source is load DF
    dict = {}
    for col_name in col_list:
        dict[col_name] = f"{source_alias_keyword}.{col_name}"
    return dict


def generate_delete_condition_for_delta_in_raw(col_name, action_code, target_alias_keyword="target",
                                               source_alias_keyword="source"):
    delete_condition = f"{source_alias_keyword}.{col_name}" + " = " + f"'{action_code}'"
    return delete_condition

def generate_merge_condition_for_ssn_handling(target_key, source_key, target_alias_keyword="target",
                                              source_alias_keyword="source"):
    # target is delta table and source is load DF
    merge_condition = '{}.{} = {}.{}'.format(target_alias_keyword, target_key, source_alias_keyword, source_key)
    return merge_condition


def generate_update_condition_for_ssn_handling(target_alias_keyword="target", source_alias_keyword="source"):
    # target is delta table and source is load DF
    return {'participant_id': f"{source_alias_keyword}.new_participant_id"}


def generate_merge_condition_for_control_table(merge_keys, target_alias_keyword="target",
                                               source_alias_keyword="source"):
    # target is delta table and source is load DF
    merge_condition = ' and '.join(
        '{}.{} = {}.{}'.format(target_alias_keyword, pkColumn, source_alias_keyword, pkColumn) for pkColumn in
        merge_keys)
    return merge_condition


def generate_update_condition_for_control_table(col_list, target_alias_keyword="target", source_alias_keyword="source"):
    # target is delta table and source is load DF
    dict = {}
    for col_name in col_list:
        dict[col_name] = f"{source_alias_keyword}.{col_name}"
    return dict


def generate_insert_condition_for_control_table(col_list, target_alias_keyword="target", source_alias_keyword="source"):
    # target is delta table and source is load DF
    dict = {}
    for col_name in col_list:
        dict[col_name] = f"{source_alias_keyword}.{col_name}"
    return dict
