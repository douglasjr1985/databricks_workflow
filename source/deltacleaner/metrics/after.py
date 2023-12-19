import logging

from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import IntegerType, LongType

class DeltaTableMetricsCollectorAfter:
    """
    Collects and analyzes metrics from Delta tables after processing.
    """
    def __init__(self, spark_session):
        self.spark = spark_session

    def get_tables_info(self):
        try:
            db_tables = self.spark.sql(
                "SELECT database AS database_name, table AS table_name FROM app_observability.vacuum_metrics WHERE date(data_execution) >= date(current_date())"
            ).rdd.map(lambda row: {'database_name': row['database_name'], 'table_name': row['table_name']}).collect()
            return [table for table in db_tables]
        except Exception as e:
            logging.error(f"Error retrieving tables: {e}")
            return []

    def table_detail_after(self, database_name, table_name):
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
        try:
            df_detail_after = self.table_detail_after(database_name, table_name)
            self.update_table(df_detail_after)
        except Exception as e:
            logging.error(f"Error collecting metrics for table {table_name} in database {database_name}: {e}")

    def collect_metrics(self):
        try:
            tables = self.get_tables_info()
            for db, tbl in tables:
                self.collect_metrics_for_table(db, tbl)
        except Exception as e:
            logging.error("Error collecting metrics for tables: {e}")