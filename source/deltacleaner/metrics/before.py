import logging
import json

from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from urllib.parse import urlparse
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import IntegerType, LongType

class DeltaTableMetricsCollectorBefore:
    """
    A class to collect and analyze metrics from Delta tables, focusing on tables
    modified on the current day.
    """

    def __init__(self, spark_session, config_file, max_threads=20):
        """
        Initializes the DeltaTableMetricsCollectorBefore instance.

        Args:
            spark_session (SparkSession): The active SparkSession.
            config_file (str): The path to the configuration file.
            max_threads (int): The maximum number of threads for parallel execution.
        """
        self.spark = spark_session
        self.config_file = config_file
        self.max_threads = max_threads
        self.config_data = self.load_config()
        self.database_names = self.config_data.get('database_name', [])
        self.skip_tables = self.config_data.get('skip_tables', [])

    def load_config(self):
        """
        Loads configuration data from a JSON file.
        """
        if not os.path.exists(self.config_file):
            logging.error(f"Config file not found: {self.config_file}")
            return {}

        try:
            with open(self.config_file, 'r') as file:
                return json.load(file)
        except Exception as e:
            logging.error(f"Failed to load config file: {e}")
            return {}

    def database_exists(self, database_name):
        """
        Checks if the specified database exists in Spark.

        Args:
            database_name (str): The name of the database.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        try:
            self.spark.sql(f"USE {database_name}")
            return True
        except AnalysisException:
            logging.warning(f"Database {database_name} does not exist.")
            return False

    def get_tables_modified_today(self):
        """
        Retrieves tables that were modified today in the specified databases.

        Returns:
            list: A list of tuples (database name, table name) for modified tables.
        """
        modified_tables = []
        current_date = datetime.now().date()

        for database_name in self.database_names:
            if self.database_exists(database_name):
                try:
                    tables_df = self.spark.sql(f"SHOW TABLES IN {database_name}")
                    for row in tables_df.collect():
                        table_name = row.tableName
                        if table_name not in self.skip_tables:
                            history_query = f"DESCRIBE HISTORY {database_name}.{table_name}"
                            history_df = self.spark.sql(history_query)
                            last_modified_row = history_df.orderBy(F.desc("timestamp")).first()

                            if last_modified_row and last_modified_row.timestamp.date() == current_date:
                                modified_tables.append((database_name, table_name))
                except Exception as e:
                    logging.error(f"Error checking modification date for tables in database {database_name}: {e}")

        return modified_tables

    def get_table_properties(self, database_name, table_name):
        """
        Retrieves properties of a table from Delta Lake.

        Args:
            database_name (str): The name of the database.
            table_name (str): The name of the table.

        Returns:
            tuple: A tuple containing the type, location, and provider of the table.
        """
        try:
            desc_query = f"DESC EXTENDED {database_name}.{table_name}"
            result_df = self.spark.sql(desc_query)
            filtered_df = result_df.filter(F.col("col_name").isin("Type", "Location", "Provider"))
            properties = {row.col_name: row.data_type for row in filtered_df.collect()}

            return properties.get('Type'), properties.get('Location'), properties.get('Provider')
        except Exception as e:
            logging.error(f"Error retrieving properties for table {table_name} in database {database_name}: {e}")
            return None, None, None

    def table_detail_before(self, database_name, table_name):
        """
        Prepares table details before processing.

        Args:
            database_name (str): The name of the database.
            table_name (str): The name of the table.

        Returns:
            DataFrame: A DataFrame containing table details.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, f'{database_name}.{table_name}')
            df_detail = delta_table.detail()
            return df_detail \
                .withColumn('data_execution', F.current_timestamp()) \
                .withColumn('database', F.lit(database_name)) \
                .withColumn('table', F.lit(table_name)) \
                .withColumn('num_files_before', df_detail['numFiles'].cast(IntegerType())) \
                .withColumn('total_size_before', df_detail['sizeInBytes'].cast(LongType()))
        except Exception as e:
            logging.error(f"Error preparing details for table {table_name} in database {database_name}: {e}")
            return None

    def save_table(self, df_detail_before):
        """
        Saves the table details into a Delta table.

        Args:
            df_detail_before (DataFrame): The DataFrame containing table details to save.
        """
        if df_detail_before:
            try:
                df_detail_before.write.format('delta').mode('append').option('mergeSchema', 'true').saveAsTable('app_observability.vacuum_metrics')
            except Exception as e:
                logging.error(f"Error saving table details: {e}")

    def collect_metrics_for_table(self, database_name, table_name):
        """
        Collects and saves metrics for a specific table.

        Args:
            database_name (str): The name of the database.
            table_name (str): The name of the table.
        """
        try:
            table_type, table_location, table_provider = self.get_table_properties(database_name, table_name)

            if table_type and table_location and table_provider:
                df_detail_before = self.table_detail_before(database_name, table_name)
                self.save_table(df_detail_before)
        except Exception as e:
            logging.error(f"Error collecting metrics for table {table_name} in database {database_name}: {e}")

    def collect_metrics(self):
        """
        Collects and saves metrics for all modified tables in the specified databases.
        """
        try:
            tables = self.get_tables_modified_today()
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = {executor.submit(self.collect_metrics_for_table, db, tbl): (db, tbl) for db, tbl in tables}
                for future in as_completed(futures):
                    db, tbl = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error collecting metrics for table {tbl} in database {db}: {e}")
        except Exception as e:
            logging.error(f"Error collecting metrics for tables: {e}")
