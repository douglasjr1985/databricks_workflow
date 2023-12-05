# Databricks notebook source
import boto3
import math
import time
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Row
from urllib.parse import urlparse
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text('database_name', '')
dbutils.widgets.text('table_name', '')

database_name = dbutils.widgets.get('database_name')
table_name = dbutils.widgets.get('table_name')

print(f'''
database_name = {database_name}
table_name = {table_name}
''')


def get_size_and_count_files(bucket_name: str, prefix: str):
    s3 = boto3.resource("s3")
    bucket_s3 = s3.Bucket(bucket_name)
    num_files = 0
    total_size = 0
    for b in bucket_s3.objects.filter(Prefix=prefix):
        total_size += b.size
        num_files += 1
        if num_files % 1000 == 0:
            print(f"num_files={num_files}, total_size={total_size}")
    print(f"num_files={num_files}, total_size={total_size}")
    return (num_files, total_size)


def is_exists_prefix(bucket_name: str, prefix: str):
    s3 = boto3.resource("s3")
    bucket_s3 = s3.Bucket(bucket_name)
    return sum(1 for _ in bucket_s3.objects.filter(Prefix=prefix).all()) > 0

def get_table_properties(database_name: str, table_name: str):
    table_properties = (
        spark.sql(f"desc extended {database_name}.{table_name}")
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
    if len(table_properties) == 0:
        raise Exception(
            f"Não foi identificada as propriedades da tabela {database_name}.{table_name}"
        )
    table_type, table_location, table_provider = table_properties.pop()
    return (table_type, table_location, table_provider)


class S3Url(object):
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket_name(self):
        return self._parsed.netloc

    @property
    def prefix(self):
        if self._parsed.query:
            return self._parsed.path.lstrip("/") + "?" + self._parsed.query
        else:
            return self._parsed.path.lstrip("/")


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "Kb", "Mb", "Gb", "Tb", "Pb", "Eb", "Zb", "Yb")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


inicio_time = time.time()
table_type, table_location, table_provider = get_table_properties(database_name=database_name, table_name=table_name)
bucket_url = S3Url(table_location)
num_files, total_size = get_size_and_count_files(bucket_url.bucket_name, bucket_url.prefix + '/')
size_human_readable = convert_size(total_size)


print(f'''
      table_type = {table_type}
      table_location = {table_location}
      table_provider = {table_provider}
      num_files = {num_files}
      total_size = {total_size}
      size_human_readable = {size_human_readable}
''')


if (is_exists_prefix(bucket_url.bucket_name, bucket_url.prefix + '/checkpoint')):
    raise Exception(f'Não é possivel continuar pois o diretorio de checkpoint está no diretório de dados [{table_location}]')

if (is_exists_prefix(bucket_url.bucket_name, bucket_url.prefix + '/schema')):
    raise Exception(f'Não é possivel continuar pois o diretorio de schemas está no diretório de dados [{table_location}]')

# COMMAND ----------

delta_table = DeltaTable.forName(spark, f'{database_name}.{table_name}')
df_detail = delta_table.detail()
display(df_detail)

# COMMAND ----------

df_detail_before = (
  df_detail
    .withColumn('data_execution', F.current_timestamp())
    .withColumn('database', F.lit(database_name))
    .withColumn('table', F.lit(table_name))
    .withColumn('num_files_before', F.lit(num_files))
    .withColumn('total_size_before', F.lit(total_size).cast('long'))
    .withColumn('size_human_readable_before', F.lit(size_human_readable))
)



spark.conf.set('spark.databricks.delta.vacuum.parallelDelete.enabled', 'true')

display(delta_table.vacuum(24*7))

num_files_after, total_size_after = get_size_and_count_files(bucket_url.bucket_name, bucket_url.prefix + '/')
size_human_readable_after = convert_size(total_size_after)

df_detail_after = (
  df_detail_before
    .withColumn('num_files_after', F.lit(num_files_after))
    .withColumn('total_size_after', F.lit(total_size_after).cast('long'))
    .withColumn('size_human_readable_after', F.lit(size_human_readable_after))
)

display(df_detail_after)


print(f'''
      table_type = {table_type}
      table_location = {table_location}
      table_provider = {table_provider}
      num_files_before = {num_files}
      total_size_before = {total_size}
      size_human_readable_before = {size_human_readable}
      num_files_after = {num_files_after}
      total_size_after = {total_size_after}
      size_human_readable_after = {size_human_readable_after}
''')

display(df_detail_after)


delta_table_after2 = DeltaTable.forName(spark, f'{database_name}.{table_name}')
df_detail_after2 = delta_table_after2.detail()
display(df_detail_after2)


(
  df_detail_after
    .withColumn('duration', F.lit(time.time() - inicio_time))
    .write
    .format('delta')
    .mode('append')
    .option('mergeSchema', 'true')
    .saveAsTable('app_observability.vacuum_metrics')
)