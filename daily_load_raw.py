__author__ = "lumiq"

from wealthcentral.utils.object_utils import RawJobArguments, RawControlTableParameters
from wealthcentral.utils.config_utils import load_json_file, add_raw_control_table_entry, create_control_file
from wealthcentral.jobs.staging_to_raw.helper_utils.config_utils import read_file_parameters
from wealthcentral.jobs.staging_to_raw.helper_utils.objects_utils import FileParameters
from wealthcentral.utils.spark_utils import read_from_catalog
from wealthcentral.jobs.staging_to_raw.helper_utils.data_utils import (
    convert_date_cols_to_valid_format,
    add_unique_key,
    identify_duplicates,
    identify_invalid_sma_fund_balance,
    identify_invalid_plan_number,
    parse_txn_data,
    replace_client,
    replace_loan_type,
    change_fund_code,
    change_price_id,
    filter_rejected_df,
    filter_valid_df,
    generate_merge_condition_for_delta_in_raw,
    generate_delete_condition_for_delta_in_raw,
    generate_update_condition_for_delta_in_raw,
    generate_insert_condition_for_delta_in_raw,
    generate_merge_condition_for_ssn_handling,
    generate_update_condition_for_ssn_handling, remove_junk_values_from_cols, fill_source_cycle_date_where_nil
)
from wealthcentral.utils.spark_utils import (
    overwrite_delta_load,
    merge_and_write_delta_load,
    get_dbutils,
    append_delta_load,
    get_current_datetime
)
import os
from pyspark.sql import SparkSession
import logging


def load_file_in_raw(spark: SparkSession, job_arguments: RawJobArguments, file_parameters: FileParameters = None):
    logger = logging.getLogger('pyspark')

    # Get Current Datetime as Job Start Time
    etl_job_start_time = get_current_datetime()

    if file_parameters is None:
        file_config_path = os.getenv('FILE_CONFIG_PATH')
        logger.info('File Config Path: %s', file_config_path)

        # Read source file config
        file_config = load_json_file(
            file_path=file_config_path
        )

        # Read File Parameters
        file_parameters = read_file_parameters(
            env=job_arguments.env,
            config_file=file_config,
            source_name=job_arguments.source_name,
            file_name=job_arguments.file_name,
        )
        logger.info(
            'Parsed File Parameters: source_name: %s, file_name: %s, file_type: %s, file_format: %s, file_segmented_flag: %s, file_load: %s, file_header_flag: %s, primary_keys: %s, table_name: %s, file_segment: %s',
            file_parameters.source_name,
            file_parameters.file_name,
            file_parameters.file_type,
            file_parameters.file_format,
            file_parameters.file_segmented_flag,
            file_parameters.file_load,
            file_parameters.file_header_flag,
            file_parameters.primary_keys,
            file_parameters.table_name,
            file_parameters.file_segment
        )


    # TODO: handle scenario where catalog returns empty df - do this by checking partition exists
    # Read from staging_to_raw database table and load the DF
    database_name = os.getenv('STAGING_DATABASE_NAME')
    table_name = file_parameters.table_name
    partition_cols = ["batch_date"]
    partition_values = [job_arguments.batch_date]

    df = read_from_catalog(
        spark=spark,
        database_name=database_name,
        table_name=table_name,
        condition_list=partition_cols,
        value_list=partition_values
    )
    logger.info('Data Read from Staging into Spark Dataframe')

    # Remove junk values from columns
    df = remove_junk_values_from_cols(
        df=df
    )
    logger.info('Successfully removed junk values from columns in Spark Dataframe')


    # Fill source_cycle_date where nil
    df = fill_source_cycle_date_where_nil(
        df=df,
        batch_date=job_arguments.batch_date
    )
    logger.info('Fill source cycle date column where nil in Spark Dataframe')


    # Convert Dates to Valid Format
    df = convert_date_cols_to_valid_format(
        df=df
    )
    logger.info('Successfully converted DateType to a valid format in Spark Dataframe')


    if job_arguments.file_name in ('TRANSACTION_VNQ_WC.TXT'):
        partition_keys = [x for x in file_parameters.primary_keys if x != 'newr_nqtxn_key']
        logger.info('Partition Keys for Transaction VNQ: %s', partition_keys)
        df = add_unique_key(
            df=df,
            partition_key=partition_keys,
            order_key='transaction_date'
        )

    if file_parameters.primary_keys is not None:
        if job_arguments.file_name not in ('TRANSACTION_VNQ_WC.TXT'):
            df = identify_duplicates(
                df=df,
                partition_key=file_parameters.primary_keys
            )


    if 'EXN.XXEWRE01.XXEWRPTD' in job_arguments.file_name:
        df = identify_invalid_sma_fund_balance(
            df=df
        )


    if job_arguments.file_name.startswith('SUPER_OMNI') or job_arguments.file_name.startswith('EXN.'):
        df = identify_invalid_plan_number(
            df=df
        )


    if 'SEQHIST' in job_arguments.file_name or 'EXN.XXEWRE01.XXEWRTRXH' in job_arguments.file_name:
        df = parse_txn_data(
            df=df
        )

    if 'SSN.OUTPUT' in job_arguments.file_name:
        df = replace_client(
            df=df
        )

    if 'EXN.XXEWRE01.XXEWRPPL' in job_arguments.file_name:
        df = replace_loan_type(
            df=df
        )

    if job_arguments.file_name == 'EXN.XXEWRE01.XXEWRPFD.NG':
        df = change_fund_code(
            df=df
        )

    if job_arguments.file_name == 'EXN.XXEWRE01.XXEWRPFD.JB':
        df = change_price_id(
            df=df
        )

    # Segregate rejected and valid records
    df.persist()
    logger.info('Performing segregation of Spark Dataframe basis Drop filter')

    if 'to_drop' in df.columns:
        valid_df = filter_valid_df(df=df)
        rejected_df = filter_rejected_df(df=df)
    else:
        valid_df = df
        rejected_df = None


    # Write rejected data in raw environment
    # Rejected data in raw environment to always be full_load basis partition on batch_date and file_segment (if present)
    if rejected_df:
        database_name = os.getenv('RAW_DATABASE_NAME')
        source_name = file_parameters.source_name
        table_name = '_'.join([file_parameters.table_name, "rejected"])

        if file_parameters.file_segmented_flag:
            partition_cols = ["batch_date", "file_segment"]
        else:
            partition_cols = ["batch_date"]

        location = '/'.join([os.getenv('RAW_OUTPUT_PATH'), source_name, table_name])

        logger.info(
            'Writing Rejected Spark Dataframe to Raw with parameters: database_name: %s, table_name: %s, partition_columns: %s, location: %s',
            database_name,
            table_name,
            partition_cols,
            location
        )

        # Write rejected dataframe load in raw environment
        overwrite_delta_load(
            spark=spark,
            df=rejected_df,
            database_name=database_name,
            table_name=table_name,
            partition_cols=partition_cols,
            location=location
        )
        logger.info('Rejected Spark Dataframe written to Raw')


    # Write valid data in raw environment
    database_name = os.getenv('RAW_DATABASE_NAME')
    source_name = file_parameters.source_name
    table_name = file_parameters.table_name
    location = '/'.join([os.getenv('RAW_OUTPUT_PATH'), source_name, table_name])

    logger.info(
        'Writing Valid Spark Dataframe to Raw with parameters: database_name: %s, table_name: %s, partition_columns: %s, location: %s',
        database_name,
        table_name,
        partition_cols,
        location
    )


    if 'SEQHIST' in job_arguments.file_name:
        # for the file_name in 'SUPER_OMNI_NEWR_SEQHIST' - use action code to perform insert, update and deletes
        merge_condition_for_delta = generate_merge_condition_for_delta_in_raw(
            merge_keys=file_parameters.primary_keys
        )
        delete_condition_for_delta = generate_delete_condition_for_delta_in_raw(
            col_name="seq_oper_code",
            action_code="D"
        )
        update_condition_for_delta = generate_update_condition_for_delta_in_raw(
            col_list=valid_df.columns
        )
        insert_condition_for_delta = generate_insert_condition_for_delta_in_raw(
            col_list=valid_df.columns
        )
        merge_and_write_delta_load(
            spark=spark,
            df=valid_df,
            database_name=database_name,
            table_name=table_name,
            merge_condition=merge_condition_for_delta,
            update_condition=update_condition_for_delta,
            insert_condition=insert_condition_for_delta,
            location=location
        )

    else:
        # if not segmented and full load - overwrite without partition
        # if segmented and full load - even then overwrite without partition - this is required cos of 0 byte segment or re-segmentation
        if file_parameters.file_load == 'full_load':
            overwrite_delta_load(
                spark=spark,
                df=valid_df,
                database_name=database_name,
                table_name=table_name,
                location=location
            )

        # if not segmented and incremental (NG) - create delta and upsert cos this will be necessary for ssn_change and ssn_merge
        # if segmented and incremental (JB) - create delta and upsert cos this will be necessary for ssn_change and ssn_merge
        if file_parameters.file_load == 'incremental':
            merge_condition_for_delta = generate_merge_condition_for_delta_in_raw(
                merge_keys=file_parameters.primary_keys
            )
            update_condition_for_delta = generate_update_condition_for_delta_in_raw(
                col_list=valid_df.columns
            )
            insert_condition_for_delta = generate_insert_condition_for_delta_in_raw(
                col_list=valid_df.columns
            )
            logger.info('merge_condition: %s, update_condition: %s, insert_condition: %s', merge_condition_for_delta, update_condition_for_delta, insert_condition_for_delta)
            merge_and_write_delta_load(
                spark=spark,
                df=valid_df,
                database_name=database_name,
                table_name=table_name,
                merge_condition=merge_condition_for_delta,
                update_condition=update_condition_for_delta,
                insert_condition=insert_condition_for_delta,
                location=location
            )

    logger.info('Valid Spark Dataframe written to Raw')


    # Un-persist DF
    df.unpersist()


    # Perform SSN change/merge if source_name = 'omni_7.4' and file_load = 'incremental'
    # To be performed as per source_channel respectively, ie, omni jb and omni ng
    # Only perform if ssn_output_{source_channel} table present in newr_raw database
    if (job_arguments.source_name == 'omni_7.4') and (file_parameters.file_load == 'incremental') and ('SEQHIST' not in job_arguments.file_name):
        logger.info(
            'Handling SSN change/merge for source_name: %s and file_name: %s',
            file_parameters.source_name,
            file_parameters.file_name
        )

        ssn_handler(
            spark=spark,
            job_arguments=job_arguments,
            file_parameters=file_parameters
        )


    # Get Current Datetime as Job End Time
    etl_job_end_time = get_current_datetime()


    # Add Control Table Entry
    control_table_df = add_raw_control_table_entry(
        spark=spark,
        control_table_parameters=RawControlTableParameters(
            source_name=file_parameters.source_name,
            file_name=file_parameters.file_name,
            batch_date=job_arguments.batch_date,
            table_name=file_parameters.table_name,
            etl_start_datetime=etl_job_start_time,
            etl_end_datetime=etl_job_end_time,
            status='SUCCESS',
            created_by='SYSTEM'
        )
    )

    database_name = os.getenv('RAW_DATABASE_NAME')
    source_name = "system"
    table_name = "control_table"
    location = '/'.join([os.getenv('RAW_OUTPUT_PATH'), source_name, table_name])

    # Write Control Table
    append_delta_load(
        spark=spark,
        df=control_table_df,
        database_name=database_name,
        table_name=table_name,
        location=location
    )

    # Create Control File
    source_name = "system"
    dir_name = "control_file"
    control_file_date = job_arguments.batch_date
    table_name = file_parameters.table_name
    location = '/'.join([os.getenv('RAW_OUTPUT_PATH'), source_name, dir_name, control_file_date, table_name])
    dbutils = get_dbutils(spark)

    create_control_file(
        dbutils=dbutils,
        file_path=location
    )


def ssn_handler(spark: SparkSession, job_arguments: RawJobArguments, file_parameters: FileParameters):
    # Get source_channel
    if "NG" in file_parameters.file_name:
        source_channel = "NG"
    elif "JB" in file_parameters.file_name:
        source_channel = "JB"
    else:
        # Do nothing
        return

    # Check if table present in newr_raw database
    database_name = os.getenv('RAW_DATABASE_NAME')
    table_name = f"ssn_output_{source_channel}"

    if not spark.catalog._jcatalog.tableExists(f"{database_name}.{table_name}"):
        return

    # ssn_output_{source_channel} if a full load table
    # Read only 850 (change) and 851 (merge) codes
    ssn_output_df = read_from_catalog(
        spark=spark,
        database_name=database_name,
        table_name=table_name,
        condition_list=["admin_tran_code"],
        value_list=[["850", "851"]]
    )

    merge_condition_for_ssn_handling = generate_merge_condition_for_ssn_handling(
        target_key="participant_id",
        source_key="old_participant_id"
    )

    update_condition_for_ssn_handling = generate_update_condition_for_ssn_handling()

    database_name = os.getenv('RAW_DATABASE_NAME')
    table_name = file_parameters.table_name

    merge_and_write_delta_load(
        spark=spark,
        df=ssn_output_df,
        database_name=database_name,
        table_name=table_name,
        merge_condition=merge_condition_for_ssn_handling,
        update_condition=update_condition_for_ssn_handling,
    )
