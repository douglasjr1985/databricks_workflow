import json
import time
import subprocess

import json
import time
import subprocess
import datetime


from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# Caminho para o arquivo JSON
arquivo_json = '/config/param.json'


# Lendo o arquivo JSON
with open(arquivo_json, 'r') as file:
    data = json.load(file)


# Função para obter o timestamp mais recente da tabela Delta
def get_max_timestamp(database_name, table_name):
    try:
        delta_table = DeltaTable.forName(spark, f"{database_name}.{table_name}")
        max_timestamp = delta_table.history().agg({"timestamp": "max"}).collect()[0][0]
        return max_timestamp
    except AnalysisException:
        print("A tabela cleaned_issuing.contas_test não existe.")    



# Lista para armazenar as informações das tabelas
tables_info = []

criterio_7_dias_atras = datetime.datetime.now() - datetime.timedelta(days=7)


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

    # Adiciona as tabelas filtradas à lista geral, verificando o timestamp
    for table in db_tables:
        max_timestamp = get_max_timestamp(table['database_name'], table['table_name'])
        if max_timestamp is not None and max_timestamp > criterio_7_dias_atras:
            tables_info.append(table)

# Agora, 'tables_info' contém as informações das tabelas atualizadas nos últimos 7 dias
print(f'Quantidade de tabelas: {len(tables_info)}')        