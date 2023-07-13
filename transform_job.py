from pyspark.sql import SparkSession
import os

from wealthcentral.jobs.raw_to_transform.helper_utils.data_utils import substitute_null_primary_keys, drop_duplicates, \
    trim_string_columns, remove_junk_values_from_cols, add_delta_op_col
from wealthcentral.jobs.raw_to_transform.helper_utils.objects_utils import JobNameToJobMapper
from wealthcentral.utils.config_utils import load_json_file, add_transform_control_table_entry, create_control_file
from wealthcentral.utils.object_utils import TransformJobArguments, TransformControlTableParameters
from wealthcentral.jobs.raw_to_transform.helper_utils.config_utils import read_transform_parameters
import logging
from wealthcentral.utils.spark_utils import overwrite_delta_load, overwrite_parquet_load, get_current_datetime, \
    append_delta_load, get_dbutils,merge_and_write_delta_load
from wealthcentral.jobs.staging_to_raw.helper_utils.data_utils import ( generate_merge_condition_for_delta_in_raw,
    generate_delete_condition_for_delta_in_raw,
    generate_update_condition_for_delta_in_raw,
    generate_insert_condition_for_delta_in_raw)

def load_file_in_transform(spark: SparkSession, job_arguments: TransformJobArguments):
    logger = logging.getLogger('pyspark')

    # Get Current Datetime as Job Start Time
    etl_job_start_time = get_current_datetime()

    transform_config_path = os.getenv('TRANSFORM_CONFIG_PATH')
    logger.info('File Config Path: %s', transform_config_path)

    # Read source file config
    transform_config = load_json_file(
        file_path=transform_config_path
    )

    # Read File Parameters
    transform_parameters = read_transform_parameters(
        config_file=transform_config,
        entity_name=job_arguments.transform_entity_name
    )


    # Set Spark Session to Use RAW Database
    database_name = os.getenv('RAW_DATABASE_NAME')
    spark.sql(f"use {database_name}")


    transform_df = None
    # Basis transform entity name, trigger job
    if job_arguments.transform_entity_name in JobNameToJobMapper:
        # get the function associated with the name and invoke it with the variable
        transform_df = JobNameToJobMapper[job_arguments.transform_entity_name](spark, transform_parameters.primary_keys)
    else:
        logger.info('Transform Entity Function Not Found')

    if transform_df:

        transform_df = trim_string_columns(
            df=transform_df
        )

        transform_df = remove_junk_values_from_cols(
            df=transform_df
        )

        if transform_parameters.primary_keys:
            # Stub primary keys in the Dataframe using default values
            transform_df = substitute_null_primary_keys(
                df=transform_df,
                primary_keys=transform_parameters.primary_keys
            )
            # Drop duplicate records
            transform_df = drop_duplicates(
                df=transform_df,
                partition_key=transform_parameters.primary_keys
            )

        # Persist DF
        transform_df.persist()

        # Write data in transform environment
        database_name = os.getenv('TRANSFORM_DATABASE_NAME')
        table_name = transform_parameters.entity_name
        location = '/'.join([os.getenv('TRANSFORM_OUTPUT_PATH'), table_name])

        if transform_parameters.entity_name=='txn':
            merge_condition_for_delta = generate_merge_condition_for_delta_in_raw(
                merge_keys=transform_parameters.primary_keys
            )
            update_condition_for_delta = generate_update_condition_for_delta_in_raw(
                col_list=transform_df.columns
            )
            insert_condition_for_delta = generate_insert_condition_for_delta_in_raw(
                col_list=transform_df.columns
            )
            delete_condition_for_delta = generate_delete_condition_for_delta_in_raw(
                col_name="seq_oper_code",
                action_code="D"
            )
            merge_and_write_delta_load(
                spark=spark,
                df=transform_df,
                database_name=database_name,
                table_name=table_name,
                merge_condition=merge_condition_for_delta,
                update_condition=update_condition_for_delta,
                delete_condition=delete_condition_for_delta,
                insert_condition=insert_condition_for_delta,
                partition_cols=["run_date"],
                location=location
            )

        else:
            if transform_parameters.entity_load == 'full_load':

                overwrite_delta_load(
                spark=spark,
                df=transform_df,
                database_name=database_name,
                table_name=table_name,
                location=location
                )

            if transform_parameters.entity_load == 'incremental':
                merge_condition_for_delta = generate_merge_condition_for_delta_in_raw(
                    merge_keys=transform_parameters.primary_keys
                )
                update_condition_for_delta = generate_update_condition_for_delta_in_raw(
                    col_list=transform_df.columns
                )
                insert_condition_for_delta = generate_insert_condition_for_delta_in_raw(
                    col_list=transform_df.columns
                )
                merge_and_write_delta_load(
                    spark=spark,
                    df=transform_df,
                    database_name=database_name,
                    table_name=table_name,
                    merge_condition=merge_condition_for_delta,
                    update_condition=update_condition_for_delta,
                    insert_condition=insert_condition_for_delta,
                    partition_cols=["source_cycle_date"],
                    location=location
                )

        # Write data in consumption stage environment
        # transform_df = add_delta_op_col(
        #     df=transform_df
        # )

        if transform_parameters.entity_name =='txn':
            df=spark.read.format("delta").load(location)
            stage_location = '/'.join([os.getenv('CONSUMPTION_STAGE_PATH'), table_name])
            overwrite_parquet_load(
                df=df,
                location=stage_location
            )
        else:
            stage_location = '/'.join([os.getenv('CONSUMPTION_STAGE_PATH'), table_name])
            overwrite_parquet_load(
                df=transform_df,
                location=stage_location
            )

    spark.catalog.dropTempView("*")
    spark.catalog.clearCache()

    # Get Current Datetime as Job Start Time
    etl_job_end_time = get_current_datetime()

    # Add Control Table Entry
    control_table_df = add_transform_control_table_entry(
        spark=spark,
        control_table_parameters=TransformControlTableParameters(
            batch_date=job_arguments.batch_date,
            table_name=transform_parameters.table_name,
            etl_start_datetime=etl_job_start_time,
            etl_end_datetime=etl_job_end_time,
            status='SUCCESS',
            created_by='SYSTEM'
        )
    )

    database_name = os.getenv('TRANSFORM_DATABASE_NAME')
    source_name = "system"
    table_name = "control_table"
    location = '/'.join([os.getenv('TRANSFORM_OUTPUT_PATH'), source_name, table_name])

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
    table_name = transform_parameters.table_name
    location = '/'.join([os.getenv('TRANSFORM_OUTPUT_PATH'), source_name, dir_name, control_file_date, table_name])
    dbutils = get_dbutils(spark)

    create_control_file(
        dbutils=dbutils,
        file_path=location
    )


    source_name = "system"
    dir_name = "control_file"
    control_file_date = job_arguments.batch_date
    table_name = transform_parameters.table_name
    location = '/'.join([os.getenv('CONSUMPTION_STAGE_PATH'), source_name, dir_name, control_file_date, table_name])
    dbutils = get_dbutils(spark)

    create_control_file(
        dbutils=dbutils,
        file_path=location
    )
