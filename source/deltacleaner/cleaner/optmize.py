import json
import logging

from concurrent.futures import ThreadPoolExecutor
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

class OptimizeJob:
    """Class to handle optimize operations on Delta tables."""

    def __init__(self, spark_session,max_threads=20):
        """
        Initialize the OptimizeJob instance.

        :param spark_session: The active SparkSession.
        :param config_file: Path to the configuration file.
        :param max_threads: Maximum number of threads for parallel execution.
        """
        self.spark = spark_session
        self.max_threads = max_threads
        self.config_data = self.load_config()
        self.tables_processed = 0
        self.total_tables = 0

    def optimize_table(self, database_name, table_name):
        """
        Perform an optimize operation on a specific Delta table.

        :param database_name: Name of the database.
        :param table_name: Name of the table to optimize.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, f"{database_name}.{table_name}")
            delta_table.optimize()
            self.tables_processed += 1
            logging.info(f"Optimize completed on {database_name}.{table_name} "
                         f"({self.tables_processed} out of {self.total_tables} tables processed)")
        except AnalysisException:
            logging.error(f"Table not found: {database_name}.{table_name}")

    def optimize_wrapper(self, table_info):
        """
        Wrapper method to call optimize_table.

        :param table_info: Dictionary containing database and table name.
        """
        self.optimize_table(table_info['database_name'], table_info['table_name'])

    def get_tables_info(self):
        """
        Retrieve information about tables to be optimized from the configuration.

        :return: A list of tables information.
        """
        tables_info = []

        for database_name in self.config_data['database_name']:
            try:
                db_tables = (
                        self.spark
                            .sql(f"select database as database_name , table as table_name from app_observability.vacuum_metrics where date(data_execution) = date(current_date())")
                            .rdd
                            .map(lambda row: {'database_name': row['database_name'], 'table_name': row['table_name']})
                            .collect()
                        )
                filtered_tables = [table for table in db_tables]

                tables_info.extend(filtered_tables)
                self.total_tables = len(tables_info)

            except AnalysisException as ae:
                logging.error(f"Error accessing database {database_name}: {ae}")
            except Exception as e:
                logging.error(f"Unexpected error processing database {database_name}: {e}")

        return tables_info

    def run_parallel_optimize(self):
        """
        Execute the optimize operation in parallel across the configured tables.
        """
        tables_info = self.get_tables_info()
        with ThreadPoolExecutor(max_workers=min(len(tables_info), self.max_threads)) as executor:
            futures = [executor.submit(self.optimize_wrapper, table_info) for table_info in tables_info]
            for future in futures:
                future.result()

        logging.info(f"Optimize operation completed on {len(tables_info)} tables.")
