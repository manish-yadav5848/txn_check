__author__ = "lumiq"

from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pytz import timezone


def get_current_datetime():
    return datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")


def init_spark_session():
    spark = SparkSession\
        .builder\
        .appName("Voya_WealthCentral") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate() \

    return spark


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


def create_database(spark, database_name):
    """
    Creates a new database in Databricks with the specified name.
    Parameters:
        database_name (str): The name of the database to create.
    Returns:
        None
    """
    # Check if the database already exists
    spark.sql(f"CREATE DATABASE IF NOT exists {database_name}")
    print(f"The '{database_name}' database has been created in catalog")


def overwrite_delta_load(spark, df, database_name, table_name, partition_cols=None, location=None):
    """
    Writes a DataFrame to a table in a specified database and table.

    Parameters:
        spark (SparkSession): The SparkSession to use for the write operation.
        df (DataFrame): The DataFrame to write to the table.
        database_name (str): The name of the database to write the table to.
        table_name (str): The name of the table to write the DataFrame to.
        partition_cols (list): A list of column names to partition the data by (optional).
        location (str): The location to write the table data to (optional).

    Returns:
        None
    """
    # Create the database and table in the Delta Lake metastore
    create_database(
        spark=spark,
        database_name=database_name
    )

    # Write the DataFrame
    writer = df\
        .write\
        .format("delta")\
        .option("path", f"{location}")\
        .option("partitionOverwriteMode", "dynamic")\
        .mode("overwrite")

    # Add partitioning if specified
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(f"{database_name}.{table_name}")


def append_delta_load(spark, df, database_name, table_name, partition_cols=None, location=None):
    """
    Writes a DataFrame to a table in a specified database and table.

    Parameters:
        spark (SparkSession): The SparkSession to use for the write operation.
        df (DataFrame): The DataFrame to write to the table.
        database_name (str): The name of the database to write the table to.
        table_name (str): The name of the table to write the DataFrame to.
        partition_cols (list): A list of column names to partition the data by (optional).
        location (str): The location to write the table data to (optional).

    Returns:
        None
    """
    # Create the database and table in the Delta Lake metastore
    create_database(
        spark=spark,
        database_name=database_name
    )

    # Write the DataFrame
    writer = df\
        .write\
        .format("delta")\
        .option("path", f"{location}")\
        .mode("append")

    # Add partitioning if specified
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(f"{database_name}.{table_name}")


def read_from_catalog(spark, database_name, table_name, condition_list=None, value_list=None):
    """
    Reads from a table in the Databricks catalog with optional conditions.
    Parameters:
        spark (SparkSession): The SparkSession object.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table to read from.
        condition_list (list): A list of column names for the conditions.
        value_list (list): A list of values for the conditions. Can be a list of lists.

    Returns:
        DataFrame: The DataFrame containing the results of the query.
    """
    # Define the Spark SQL query template
    query_template = f"SELECT * FROM {database_name}.{table_name}"

    # Add conditions to the query if provided
    if condition_list and value_list:
        conditions = []
        for i, col_name in enumerate(condition_list):
            if isinstance(value_list[i], str):
                conditions.append(f"{col_name} = '{value_list[i]}'")
            elif isinstance(value_list[i], list):
                list_variables = ', '.join([f"'{value}'" for value in value_list[i]])
                conditions.append(f"{col_name} IN ({list_variables})")
        if conditions:
            query_template += f" WHERE {' AND '.join(conditions)}"

    print(query_template)

    # Read the table into a DataFrame
    df = spark.sql(query_template)

    return df


# TODO: Optimize the approach in below function
def merge_and_write_delta_load(spark, df, database_name, table_name, merge_condition=None, delete_condition=None, update_condition=None, insert_condition=None, location=None, partition_cols=None):
    """
    Handles inserts, updates, and deletes in a Delta table using the merge function.

    Parameters:
        df (DataFrame): The DataFrame to write to the Delta table.
        database_name (str): The name of the database that contains the Delta table.
        table_name (str): The name of the Delta table to write the DataFrame to.
        delta_location (str): The file system location of the Delta table.

    Returns:
        None
    """
    create_database(
        spark=spark,
        database_name=database_name
    )


    if location is not None:
        # Create the DeltaTable object, or create an empty Delta table if it doesn't exist
        try:
            delta_table = DeltaTable.forPath(spark, location)
        except:
            schema = df.schema
            if partition_cols:
                DeltaTable.create(spark) \
                    .tableName(f"{database_name}.{table_name}") \
                    .location(location) \
                    .addColumns(schema) \
                    .partitionedBy(*partition_cols) \
                    .execute()
            else:
                DeltaTable.create(spark) \
                    .tableName(f"{database_name}.{table_name}") \
                    .location(location) \
                    .addColumns(schema) \
                    .execute()
            delta_table = DeltaTable.forPath(spark, location)
    else:
        delta_table = DeltaTable.forName(spark, '{}.{}'.format(database_name, table_name))

    # Merge the changes into the Delta table
    writer = delta_table.alias("target")

    if merge_condition:
        writer = writer.merge(df.alias("source"), merge_condition)

    if delete_condition:
        writer = writer.whenMatchedDelete(condition=delete_condition)

    if update_condition:
        writer = writer.whenMatchedUpdate(set=update_condition)

    if insert_condition:
        writer = writer.whenNotMatchedInsert(values=insert_condition)

    writer.execute()

    # Checkpoint - below code to make sure delta table gets created
    try:
        delta_table = DeltaTable.forName(spark, '{}.{}'.format(database_name, table_name))
    except:
        schema = df.schema
        if partition_cols:
            DeltaTable.create(spark) \
                .tableName(f"{database_name}.{table_name}") \
                .location(location) \
                .addColumns(schema) \
                .partitionedBy(*partition_cols) \
                .execute()
        else:
            DeltaTable.create(spark) \
                .tableName(f"{database_name}.{table_name}") \
                .location(location) \
                .addColumns(schema) \
                .execute()

    # Vacuum the Delta table
    #delta_table.vacuum()

    # Optimize the Delta table
    #delta_table.optimize()


def read_delta_from_catalog(spark, database_name, table_name):
    return DeltaTable.forName(spark, '{}.{}'.format(database_name, table_name))


def overwrite_parquet_load(df, partition_cols=None, location=None):
    # Write the DataFrame
    if partition_cols:
        df \
            .write \
            .partitionBy(partition_cols) \
            .option("partitionOverwriteMode", "dynamic") \
            .mode("overwrite") \
            .parquet(f"{location}")
    else:
        df \
            .write \
            .option("partitionOverwriteMode", "dynamic") \
            .mode("overwrite") \
            .parquet(f"{location}")
