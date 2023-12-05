import json
import time
# Caminho para o arquivo JSON
arquivo_json = '/Workspace/Users/douglas.dos.ext@dock.tech/notebook/param.json'

# Lendo o arquivo JSON
with open(arquivo_json, 'r') as file:
    data = json.load(file)


# Lista para armazenar as informações das tabelas
tables = []

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
    tables.extend(db_tables)


print(tables)