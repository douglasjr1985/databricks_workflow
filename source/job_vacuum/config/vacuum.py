import json
import logging

from concurrent.futures import ThreadPoolExecutor
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

class VacuumJob:
    def __init__(self, spark_session, config_file, max_threads=20):
        self.spark = spark_session
        self.config_file = config_file
        self.max_threads = max_threads
        self.config_data = self.load_config()
        self.tables_processed = 0
        self.total_tables = 0  # This will be set when you get tables info


    def load_config(self):
        with open(self.config_file, 'r') as file:
            return json.load(file)

    def vacuum_table(self, database_name, table_name, retention_hours=24*7):
        try:
            delta_table = DeltaTable.forName(self.spark, f"{database_name}.{table_name}")
            delta_table.vacuum(retention_hours)
            self.tables_processed += 1
            logging.info(f"Vacuum concluído em {database_name}.{table_name} "
                         f"({self.tables_processed} de {self.total_tables} tabelas processadas)")
        except AnalysisException:
            logging.error(f"Tabela {database_name}.{table_name} não encontrada.")

    def vacuum_wrapper(self, table_info):
        self.vacuum_table(table_info['database_name'], table_info['table_name'])

    def get_tables_info(self):
        tables_info = []

        for database_name in self.config_data['database_name']:
            try:
                db_tables = (
                    self.spark
                    .sql(f"SHOW TABLES FROM {database_name}")
                    .rdd
                    .map(lambda row: {'database_name': row['database'], 'table_name': row['tableName']})
                    .collect()
                )
                filtered_tables = [
                    table for table in db_tables 
                    if table['table_name'] not in self.config_data['skip_tables']
                ]

                tables_info.extend(filtered_tables)
                self.total_tables = len(tables_info)

            except AnalysisException as ae:
                logging.error(f"Error accessing database {database_name}: {ae}\n")
            except Exception as e:
                logging.error(f"Unexpected error processing database {database_name}: {e}\n")

        return tables_info


    def run_parallel_vacuum(self):
        tables_info = self.get_tables_info()
        with ThreadPoolExecutor(max_workers=min(len(tables_info), self.max_threads)) as executor:
            futures = [executor.submit(self.vacuum_wrapper, table_info) for table_info in tables_info]
            for future in futures:
                future.result()

        print(f"Vacuum realizado em {len(tables_info)} tabelas.")

