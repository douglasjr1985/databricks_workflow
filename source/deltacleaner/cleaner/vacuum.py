import json
import logging

from concurrent.futures import ThreadPoolExecutor
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

class VacuumJob:
    """Class to handle vacuum operations on Delta tables."""

    def __init__(self, spark_session, config_file, max_threads=20):
        """
        Initialize the VacuumJob instance.

        :param spark_session: The active SparkSession.
        :param config_file: Path to the configuration file.
        :param max_threads: Maximum number of threads for parallel execution.
        """
        self.spark = spark_session
        self.config_file = config_file
        self.max_threads = max_threads
        self.config_data = self.load_config()
        self.tables_processed = 0
        self.total_tables = 0

    def load_config(self):
        """Load configuration data from a JSON file."""
        with open(self.config_file, 'r') as file:
            return json.load(file)

    def check_need_for_vacuum(self, database_name, table_name, threshold=1000):
        history_df = self.spark.sql(f"DESCRIBE HISTORY `{database_name}`.`{table_name}`")

        return history_df.count() > threshold

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