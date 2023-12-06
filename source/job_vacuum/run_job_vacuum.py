import json
import time
import subprocess

import json
import time
import subprocess
import datetime


from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from concurrent.futures import ProcessPoolExecutor
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


import os;
os.getcwd()

# Caminho para o arquivo JSON
#arquivo_json = '/config/param.json'


# Lendo o arquivo JSON
#with open(arquivo_json, 'r') as file:
#    data = json.load(file)

# Lista para armazenar as informações das tabelas
tables_info = []

# Iterando sobre os bancos de dados
for database_name in data['database_name']:
    # Obtém as tabelas do banco de dados
    db_tables = (
        spark
        .sql(f"SHOW TABLES FROM {database_name}")
        .rdd
        .map(lambda row: {'database_name': row['database'], 'table_name': row['tableName']})
        .collect()
    )

    # Filtra as tabelas a serem puladas
    db_tables = [table for table in db_tables if table['table_name'] not in data['skip_tables']]

    # Adiciona as tabelas filtradas à lista geral
    tables_info.extend(db_tables)


# Agora, 'tables' contém as informações das tabelas de todos os bancos de dados, exceto aquelas que são para serem puladas
print(f'quantidade de tabelas: {len(tables_info)}')

# Função para inicializar o Spark e executar o VACUUM)
def vacuum_table(database_name, table_name, retention_hours):
    spark = SparkSession.builder.appName("VacuumJob").getOrCreate()
    delta_table = DeltaTable.forName(spark, f"{database_name}.{table_name}")
    delta_table.vacuum(retention_hours)
    #spark.stop()


# Função para extrair informações e chamar vacuum_table
def vacuum_wrapper(table_info):
    database_name = table_info['database_name']
    table_name = table_info['table_name']
    retention_hours = 24 * 7  # Defina o período de retenção conforme necessário
    print(f"Vacuuming {database_name}.{table_name}")
    vacuum_table(database_name, table_name, retention_hours)

# Usando ThreadPoolExecutor para executar vacuum_table em paralelo
with ThreadPoolExecutor(max_workers=len(tables_info)) as executor:
    futures = [executor.submit(vacuum_wrapper, table_info) for table_info in tables_info]

    # Aguardar a conclusão de todas as tarefas
    for future in futures:
        future.result()  # Isso irá bloquear até que a tarefa correspondente seja concluída
