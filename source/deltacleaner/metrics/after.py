import logging
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import IntegerType, LongType

class DeltaTableMetricsCollectorAfter:
    """
    Collects and analyzes metrics from Delta tables after processing.
    Designed to be used in a Databricks environment.
    """

    def __init__(self, spark_session):
        """
        Initializes the metrics collector with a Spark session and max thread count.

        Args:
            spark_session (SparkSession): The active SparkSession.
            max_threads (int): Maximum number of threads for parallel processing.
        """
        self.spark = spark_session


    def get_tables_info(self):
        """
        Fetches the list of tables to process from the 'app_observability.vacuum_metrics' table.

        Returns:
            List[Dict[str, str]]: A list of dictionaries with database and table names.
        """
        tables_info = []
        try:
            db_tables = self.spark.sql(
                "SELECT database AS database_name, table AS table_name FROM app_observability.vacuum_metrics WHERE date(data_execution) >= date(current_date())"
            ).rdd.map(lambda row: {'database_name': row['database_name'], 'table_name': row['table_name']}).collect()
            tables_info.extend(db_tables)
            return tables_info
        except Exception as e:
            logging.error(f"Error retrieving tables: {e}")
            return []

    def table_detail_after(self, database_name, table_name):
        """
        Prepares detailed information about a specific table after processing.

        Args:
            database_name (str): Name of the database containing the table.
            table_name (str): Name of the table to collect metrics for.

        Returns:
            DataFrame: A DataFrame containing detailed information about the table.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, f'{database_name}.{table_name}')
            df_detail = delta_table.detail()
            return (df_detail.withColumn('data_execution', F.current_timestamp())
                            .withColumn('database', F.lit(database_name))
                            .withColumn('table', F.lit(table_name))
                            .withColumn('num_files_after', F.col('numFiles').cast(IntegerType()))
                            .withColumn('total_size_after', F.col('sizeInBytes').cast(LongType())))
        except Exception as e:
            logging.error(f"Error preparing details for table {table_name} in database {database_name}: {e}")
            return None

    def update_table(self, df_detail_after):
        """
        Updates the 'app_observability.vacuum_metrics' table with the latest metrics.

        Args:
            df_detail_after (DataFrame): DataFrame containing the updated metrics.
        """
        if df_detail_after:
            try:
                delta_table = DeltaTable.forName(self.spark, 'app_observability.vacuum_metrics')
                merge_condition = "delta_table.id = df_detail_after.id AND delta_table.createdAt = df_detail_after.createdAt"
                (delta_table.alias('delta_table')
                 .merge(df_detail_after.alias('df_detail_after'), merge_condition)
                 .whenMatchedUpdate(set={"num_files_after": "df_detail_after.num_files_after", "total_size_after": "df_detail_after.total_size_after"})
                 .execute())
            except Exception as e:
                logging.error(f"Error updating table details: {e}")

    def collect_metrics_for_table(self, database_name, table_name):
        """
        Collects and saves metrics for a single table.

        Args:
            database_name (str): Name of the database containing the table.
            table_name (str): Name of the table to collect metrics for.
        """
        try:
            df_detail_after = self.table_detail_after(database_name, table_name)
            self.update_table(df_detail_after)
        except Exception as e:
            logging.error(f"Error collecting metrics for table {table_name} in database {database_name}: {e}")

    def after_wrapper(self, table_info):
        """
        Wrapper method to call optimize_table.

        :param table_info: Dictionary containing database and table name.
        """
        self.collect_metrics_for_table(table_info['database_name'], table_info['table_name'])                    

    def collect_metrics(self):
        """
        Collects and saves metrics for all tables identified for processing.
        """
        tables = self.get_tables_info()
        for table_info in tables:
            self.after_wrapper(table_info)
