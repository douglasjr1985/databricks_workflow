import logging

from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import IntegerType, LongType

class DeltaTableMetricsCollectorAfter:
    """
    Class to collect and analyze metrics from Delta tables after processing.
    """

    def __init__(self, spark_session, max_threads=20):
        """
        Initializes the collector with the Spark session and the maximum number of threads.

        :param spark_session: The active Spark session.
        :param max_threads: The maximum number of threads for parallel execution.
        """
        self.spark = spark_session
        self.max_threads = max_threads

    def get_tables_info(self):
        """
        Returns a list of (database name, table name) pairs for all specified databases.
        """
        tables_info = []
        try:
            db_tables = (
                        self.spark
                            .sql("SELECT database AS database_name, table AS table_name FROM app_observability.vacuum_metrics WHERE date(data_execution) >= date(current_date())")
                            .rdd
                            .map(lambda row: {'database_name': row['database_name'], 'table_name': row['table_name']})
                            .collect()
                        )
            filtered_tables = [table for table in db_tables]

            tables_info.extend(filtered_tables)

        except Exception as e:
            logging.error(f"Error retrieving tables: {e}")
        return tables_info

    def table_detail_after(self, database_name, table_name):
        """
        Prepares table details after processing.

        :param database_name: The name of the database.
        :param table_name: The name of the table.
        :return: A DataFrame with table details.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, f'{database_name}.{table_name}')
            df_detail = delta_table.detail()
            return (df_detail
                    .withColumn('data_execution', F.current_timestamp())
                    .withColumn('database', F.lit(database_name))
                    .withColumn('table', F.lit(table_name))
                    .withColumn('num_files_after', df_detail['numFiles'].cast(IntegerType()))
                    .withColumn('total_size_after', df_detail['sizeInBytes'].cast(LongType())))
        except Exception as e:
            logging.error(f"Error preparing details for table {table_name} in database {database_name}: {e}")
            return None

    def update_table(self, df_detail_after):
        """
        Saves the table details.

        :param df_detail_after: The DataFrame with table details after processing.
        """
        if df_detail_after:
            try:
                delta_table = DeltaTable.forName(self.spark, 'app_observability.vacuum_metrics')
                merge_condition = "delta_table.id = df_detail_after.id AND delta_table.createdAt = df_detail_after.createdAt"
                (delta_table.alias('delta_table')
                 .merge(df_detail_after.alias('df_detail_after'), merge_condition)
                 .whenMatchedUpdate(set={
                     "num_files_after": "df_detail_after.num_files_after",
                     "total_size_after": "df_detail_after.total_size_after"
                 })
                 .execute())
            except Exception as e:
                logging.error(f"Error saving table details: {e}")

    def collect_metrics_for_table(self, database_name, table_name):
        """
        Collects and saves metrics for a specific table.

        :param database_name: The name of the database.
        :param table_name: The name of the table.
        """
        try:
            df_detail_after = self.table_detail_after(database_name, table_name)
            self.update_table(df_detail_after)
        except Exception as e:
            logging.error(f"Error collecting metrics for table {table_name} in database {database_name}: {e}")

    def collect_metrics(self):
        """
        Collects and saves metrics for all tables in all specified databases synchronously.
        """
        try:
            tables = self.get_tables_info()
            for db, tbl in tables:
                self.collect_metrics_for_table(db, tbl)
        except Exception as e:
            logging.error(f"Error collecting metrics for tables: {e}")
