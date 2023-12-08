import boto3
import math
import time
import logging
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from urllib.parse import urlparse
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed

class S3Url:
    """Classe para analisar e lidar com URLs do S3."""
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket_name(self):
        """Retorna o nome do bucket da URL do S3."""
        return self._parsed.netloc

    @property
    def prefix(self):
        """Retorna o prefixo da URL do S3."""
        return self._parsed.path.lstrip("/") + "?" + self._parsed.query if self._parsed.query else self._parsed.path.lstrip("/")

class DeltaTableMetricsCollector:
    """Classe para coletar e analisar métricas de tabelas Delta."""

    def __init__(self, spark_session, config_file, max_threads=20):
        self.spark = spark_session
        self.config_file = config_file
        self.max_threads = max_threads
        self.config_data = self.load_config()
        self.database_names = self.config_data['database_name']
        self.skip_tables = self.config_data['skip_tables']

    def load_config(self):
        """Carrega dados de configuração de um arquivo JSON."""
        with open(self.config_file, 'r') as file:
            return json.load(file)

    @staticmethod
    def get_size_and_count_files(bucket_name, prefix):
        """Obtém o tamanho e a contagem de arquivos em um bucket do S3 com o prefixo dado."""
        s3 = boto3.resource("s3")
        bucket_s3 = s3.Bucket(bucket_name)
        num_files = total_size = 0
        for b in bucket_s3.objects.filter(Prefix=prefix):
            total_size += b.size
            num_files += 1
        return num_files, total_size

    @staticmethod
    def convert_size(size_bytes):
        """Converte um tamanho em bytes para um formato legível por humanos."""
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        return f"{round(size_bytes / math.pow(1024, i), 2)} {size_name[i]}"

    def get_tables_in_databases(self):
        """Retorna uma lista de pares (nome do banco de dados, nome da tabela) para todos os bancos de dados especificados."""
        all_tables = []
        for database_name in self.database_names:
            tables_df = self.spark.sql(f"SHOW TABLES IN {database_name}")
            for row in tables_df.collect():
                if row.tableName not in self.skip_tables:
                    all_tables.append((database_name, row.tableName))
        return all_tables

    def get_table_properties(self, database_name, table_name):
        """Retorna as propriedades de uma tabela."""
        table_properties = (
            self.spark.sql(f"DESC EXTENDED {database_name}.{table_name}")
            .filter(F.col("col_name").isin("Type", "Location", "Provider"))
            .withColumn("database", F.lit(database_name))
            .withColumn("table", F.lit(table_name))
            .groupBy("database", "table")
            .pivot("col_name")
            .agg(F.first("data_type"))
            .select("database", "table", "Type", "Location", "Provider")
            .rdd.map(lambda row: (row["Type"], row["Location"], row["Provider"]))
            .collect()
        )
        table_type, table_location, table_provider = table_properties.pop()
        return table_type, table_location, table_provider

    def table_detail_before(self, database_name, table_name, num_files, total_size, size_human_readable):
        delta_table = DeltaTable.forName(self.spark, f'{database_name}.{table_name}')
        df_detail = delta_table.detail()
        df_detail_before = (
            df_detail
            .withColumn('data_execution', F.current_timestamp())
            .withColumn('database', F.lit(database_name))
            .withColumn('table', F.lit(table_name))
            .withColumn('num_files_before', F.lit(num_files))
            .withColumn('total_size_before', F.lit(total_size).cast('long'))
            .withColumn('size_human_readable_before', F.lit(size_human_readable))
        )
        return df_detail_before

    def save_table(self, df_detail_before):
        df_detail_before.write.format('delta').mode('append').option('mergeSchema', 'true').saveAsTable('app_observability.vacuum_metrics')

    def collect_metrics_for_table(self, database_name, table_name):
        try:
            table_type, table_location, table_provider = self.get_table_properties(database_name, table_name)
            bucket_url = S3Url(table_location)
            num_files_before, total_size_before = self.get_size_and_count_files(bucket_url.bucket_name, bucket_url.prefix)
            size_human_readable_before = self.convert_size(total_size_before)

            df_detail_before = self.table_detail_before(database_name, table_name, num_files_before, total_size_before, size_human_readable_before)
            self.save_table(df_detail_before)
        except Exception as e:
            logging.error(f"Erro ao coletar métricas para a tabela {table_name}: {e}")

    def collect_metrics(self):
        """Coleta e salva métricas para todas as tabelas em todos os bancos de dados especificados."""
        try:
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                tables = self.get_tables_in_databases()
                futures = {executor.submit(self.collect_metrics_for_table, db, tbl): (db, tbl) for db, tbl in tables}
                for future in as_completed(futures):
                    db, tbl = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Erro ao coletar métricas para a tabela {tbl} no banco de dados {db}: {e}")
        except Exception as e:
            logging.error(f"Erro ao coletar métricas das tabelas: {e}")
