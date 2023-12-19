import json
import logging

from concurrent.futures import ThreadPoolExecutor
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

class VacuumJob:
    """Class to handle vacuum operations on Delta tables."""

    def __init__(self, spark_session, max_threads=20):
        """
        Initialize the VacuumJob instance.

        :param spark_session: The active SparkSession.
        :param config_file: Path to the configuration file.
        :param max_threads: Maximum number of threads for parallel execution.
        """
        self.spark = spark_session
        self.max_threads = max_threads
        self.tables_processed = 0
        self.total_tables = 0

    def check_need_for_vacuum(self, database_name, table_name, threshold=200):
        """
        Check whether a specific table requires vacuum operation, based on a defined threshold.

        Arguments:
            database_name (str): Name of the database.
            table_name (str): Name of the table.
            limit (int): Limit for the number of UPDATE, DELETE and MERGE operations.

        Returns:
            bool: Returns True if the number of transactions exceeds the limit, reducing the need for a vacuum.
        """
        try:
            history_df = self.spark.sql(f"DESCRIBE HISTORY `{database_name}`.`{table_name}`")
            return history_df.count() > threshold
        except Exception as e:
            logging.error(f"Error when checking the need for vacuum in the table{table_name}: {e}")
            return False


    def vacuum_table(self, database_name, table_name, retention_hours=24*7):
        """
        Perform a vacuum operation on a specific Delta table.

        :param database_name: Name of the database.
        :param table_name: Name of the table to vacuum.
        :param retention_hours: Data retention period in hours.
        """
        try:
                if self.check_need_for_vacuum(database_name, table_name):
                    delta_table = DeltaTable.forName(self.spark, f"{database_name}.{table_name}")
                    delta_table.vacuum(retention_hours)
                    logging.info(f"Vacuum completed on {database_name}.{table_name}")
                else:
                    logging.info(f"No need for vacuum on {database_name}.{table_name}")
        except AnalysisException:
            logging.error(f"Table not found: {database_name}.{table_name}")

    def vacuum_wrapper(self, table_info):
        """
        Wrapper method to call vacuum_table.

        :param table_info: Dictionary containing database and table name.
        """
        self.vacuum_table(table_info['database_name'], table_info['table_name'])

    def get_tables_info(self):
        """
        Retrieve information about tables to be optimized from the configuration.

        :return: A list of tables information.
        """
        tables_info = []

        try:
            db_tables = (
                self.spark
                    .sql("SELECT database AS database_name, table AS table_name FROM app_observability.vacuum_metrics WHERE date(data_execution) = date(current_date())")
                    .rdd
                    .map(lambda row: {'database_name': row['database_name'], 'table_name': row['table_name']})
                    .collect()
            )
            tables_info.extend(db_tables)
            self.total_tables = len(tables_info)

        except AnalysisException as ae:
            logging.error(f"AnalysisException encountered: {ae}")

        except Exception as e:
            logging.error(f"General exception encountered: {e}")

        return tables_info

    def run_parallel_vacuum(self):
        """
        Execute the vacuum operation in parallel across the configured tables.
        """
        tables_info = self.get_tables_info()
        with ThreadPoolExecutor(max_workers=min(len(tables_info), self.max_threads)) as executor:
            futures = [executor.submit(self.vacuum_wrapper, table_info) for table_info in tables_info]
            for future in futures:
                future.result()

        logging.info(f"Vacuum operation completed on {len(tables_info)} tables.")