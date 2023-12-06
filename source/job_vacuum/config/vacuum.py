from concurrent.futures import ProcessPoolExecutor
from delta.tables import DeltaTable
from pyspark.sql import SparkSession



# Função para inicializar o Spark e executar o VACUUM
def vacuum_table(database_name, table_name, retention_hours):
    spark = SparkSession.builder.appName("VacuumJob").getOrCreate()
    delta_table = DeltaTable.forName(spark, f"{database_name}.{table_name}")
    delta_table.vacuum(retention_hours)
    spark.stop()


# Função para extrair informações e chamar vacuum_table
def vacuum_wrapper(table_info):
    database_name = table_info['database_name']
    table_name = table_info['table_name']
    retention_hours = 24 * 7  # Defina o período de retenção conforme necessário
    print(f"Vacuuming {database_name}.{table_name}")
    vacuum_table(database_name, table_name, retention_hours)

# Usando ProcessPoolExecutor para executar vacuum_table em paralelo
with ProcessPoolExecutor(max_workers=len(tables_info)) as executor:
    futures = [executor.submit(vacuum_wrapper, table_info) for table_info in tables_info]

    while futures:
        for future in futures:
            if future.running():
                print(f"Uma tarefa ainda está em execução {future.result()}")
            elif future.done():
                print(f"Tarefa concluída com resultado: {future.result()}")
                futures.remove(future)

        time.sleep(0.5)  # Espera antes de verificar novamente

    # # Aguardar a conclusão de todas as tarefas
    # for future in futures:
    #     future.result()  # Isso irá bloquear até que a tarefa correspondente seja concluída    