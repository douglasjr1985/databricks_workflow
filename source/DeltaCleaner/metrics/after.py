import boto3
import math
import logging
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from urllib.parse import urlparse
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import IntegerType,LongType


class DeltaTableMetricsCollectorAfter:
    """
    Class to collect and analyze metrics from Delta tables.
    """
    def __init__(self, spark_session, config_file, max_threads=60):
        """
        Initializes the DeltaTableMetricsCollectorafter with Spark session,
        configuration file, and maximum number of threads.
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
        try:
            with open(self.config_file, 'r') as file:
                return json.load(file)
        except Exception as e:
            logging.error(f"Failed to load config file: {e}")
            return {}


    @staticmethod
    def convert_size(size_bytes):
        """
        Converts a size in bytes to a human-readable format.
        """
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        return f"{round(size_bytes / math.pow(1024, i), 2)} {size_name[i]}"
   
    def database_exists(self, database_name):
        """
        Checks if the specified database exists.
        """
        try:
            self.spark.sql(f"USE {database_name}")
            return True
        except AnalysisException:
            logging.warning(f"Database {database_name} does not exist.")
            return False
        
    def get_tables_in_databases(self):
        """
        Returns a list of (database name, table name) pairs for all specified databases.
        """
        all_tables = []
        for database_name in self.database_names:
            if self.database_exists(database_name):
                try:
                    tables_df = self.spark.sql(f"select database as database_name , table as table_name from app_observability.vacuum_metrics where date(data_execution) = date(current_date()) and database = '{database_name}'")
                    all_tables.extend([(database_name, row.tableName) for row in tables_df.collect() if row.tableName not in self.skip_tables])
                except Exception as e:
                    logging.error(f"Error retrieving tables from database {database_name}: {e}")
        return all_tables


    def get_table_properties(self, database_name, table_name):
        """
        Returns the properties of a table.
        """
        try:
            # Executar consulta SQL para obter propriedades estendidas da tabela
            desc_query = f"DESC EXTENDED {database_name}.{table_name}"
            result_df = self.spark.sql(desc_query)

            # Filtrar o DataFrame para as propriedades necessárias
            filtered_df = result_df.filter(
                F.col("col_name").isin("Type", "Location", "Provider")
            )

            # Coletar os resultados em um dicionário
            properties = {row.col_name: row.data_type for row in filtered_df.collect()}

            # Retornar as propriedades necessárias
            return properties.get('Type'), properties.get('Location'), properties.get('Provider')
        except Exception as e:
            logging.error(f"Error retrieving properties for table {table_name} in database {database_name}: {e}")
            return None, None, None



    def table_detail_after(self, database_name, table_name):
        """
        Prepares table details after processing.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, f'{database_name}.{table_name}')
            df_detail = delta_table.detail()
            return df_detail.withColumn('data_execution', F.current_timestamp()) \
                            .withColumn('database', F.lit(database_name)) \
                            .withColumn('table', F.lit(table_name)) \
                            .withColumn('num_files_after', df_detail['numFiles'].cast(IntegerType())) \
                            .withColumn('total_size_after',df_detail['sizeInBytes'].cast(LongType()))
                            
        except Exception as e:
            logging.error(f"Error preparing details for table {table_name} in database {database_name}: {e}")
            return None

    def save_table(self, df_detail_after):
            """
            Saves the table details.    
            """
            delta_table = DeltaTable.forName(spark, 'app_observability.vacuum_metrics')
            merge_condition = "delta_table.id = df_detail_after.id and  delta_table.createdAt = df_detail_after.createdAt "

            if df_detail_after:
                try:
                    # Executa a operação de mesclagem
                    (
                        delta_table.alias('delta_table')
                        .merge(df_detail_after.alias('df_detail_after'), merge_condition)
                        .whenMatchedUpdateAll()  # Atualiza as linhas correspondentes
                        .whenNotMatchedInsertAll()  # Insere linhas não correspondentes
                        .execute()
                    )

                except Exception as e:
                    logging.error(f"Error saving table details: {e}")

    def collect_metrics_for_table(self, database_name, table_name):
        """
        Collects and saves metrics for a specific table.
        """
        try:
            table_type, table_location, table_provider = self.get_table_properties(database_name, table_name)
            
            if table_type and table_location and table_provider:
                df_detail_after = self.table_detail_after(database_name, table_name)
                self.save_table(df_detail_after)
        except Exception as e:
            logging.error(f"Error collecting metrics for table {table_name} in database {database_name}: {e}")

    def collect_metrics(self):
        """
        Collects and saves metrics for all tables in all specified databases.
        """
        try:
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                tables = self.get_tables_in_databases()
                futures = {executor.submit(self.collect_metrics_for_table, db, tbl): (db, tbl) for db, tbl in tables}
                for future in as_completed(futures):
                    db, tbl = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error collecting metrics for table {tbl} in database {db}: {e}")
        except Exception as e:
            logging.error(f"Error collecting metrics for tables: {e}")
            

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    spark = SparkSession.builder.appName("DeltaTableMetricsCollectorAfter").getOrCreate()

    config_file_path = "../config/param.json"  
    collector = DeltaTableMetricsCollectorAfter(spark, config_file_path)
    collector.collect_metrics()