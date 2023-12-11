import boto3
import math
import logging
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from urllib.parse import urlparse
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed

class S3Url:
    """
    Class to parse and handle S3 URLs.
    """
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket_name(self):
        """
        Returns the bucket name from the S3 URL.
        """
        return self._parsed.netloc

    @property
    def prefix(self):
        """
        Returns the prefix from the S3 URL.
        """
        return self._parsed.path.lstrip("/") + "?" + self._parsed.query if self._parsed.query else self._parsed.path.lstrip("/")

class DeltaTableMetricsCollectorBefore:
    """
    Class to collect and analyze metrics from Delta tables.
    """
    def __init__(self, spark_session, config_file, max_threads=60):
        self.spark = spark_session
        self.config_file = config_file
        self.max_threads = max_threads
        self.config_data = self.load_config()
        self.database_names = self.config_data['database_name']
        self.skip_tables = self.config_data['skip_tables']

    def load_config(self):
        """
        Loads configuration data from a JSON file.
        """
        with open(self.config_file, 'r') as file:
            return json.load(file)

    @staticmethod
    def get_size_and_count_files(bucket_name, prefix):
        """
        Gets the size and count of files in an S3 bucket with the given prefix.
        """
        s3 = boto3.resource("s3")
        bucket_s3 = s3.Bucket(bucket_name)
        num_files = total_size = 0
        for b in bucket_s3.objects.filter(Prefix=prefix):
            total_size += b.size
            num_files += 1
        return num_files, total_size

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
        Checks if the database exists.
        """
        try:
            self.spark.sql(f"USE {database_name}")
            return True
        except AnalysisException:
            logging.warning(f"The database {database_name} does not exist.")
            return False
        
    def get_tables_in_databases(self):
        """
        Returns a list of (database name, table name) pairs for all specified databases.
        """
        all_tables = []
        for database_name in self.database_names:
            if self.database_exists(database_name):
                tables_df = self.spark.sql(f"SHOW TABLES IN {database_name}")
                all_tables.extend([(database_name, row.tableName) for row in tables_df.collect() if row.tableName not in self.skip_tables])
        return all_tables

    def get_table_properties(self, database_name, table_name):
        """
        Returns the properties of a table.
        """
        try:
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
            if table_properties:
                return table_properties.pop()
            else:
                logging.error(f"No properties found for table {table_name} in database {database_name}")
                return None, None, None
        except Exception as e:
            logging.error(f"Error retrieving properties for table {table_name} in database {database_name}: {e}")
            return None, None, None

    def table_detail_before(self, database_name, table_name, num_files, total_size, size_human_readable):
        """
        Prepares table details before processing.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, f'{database_name}.{table_name}')
            df_detail = delta_table.detail()
            return df_detail.withColumn('data_execution', F.current_timestamp()) \
                            .withColumn('database', F.lit(database_name)) \
                            .withColumn('table', F.lit(table_name)) \
                            .withColumn('num_files_before', F.lit(num_files)) \
                            .withColumn('total_size_before', F.lit(total_size).cast('long')) \
                            .withColumn('size_human_readable_before', F.lit(size_human_readable))
        except Exception as e:
            logging.error(f"Error preparing details for table {table_name} in database {database_name}: {e}")
            return None

    def save_table(self, df_detail_before):
        """
        Saves the table details.
        """
        if df_detail_before:
            try:
                df_detail_before.write.format('delta').mode('append').option('mergeSchema', 'true').saveAsTable('app_observability.vacuum_metrics')
            except Exception as e:
                logging.error(f"Error saving table details: {e}")

    def collect_metrics_for_table(self, database_name, table_name):
        """
        Collects and saves metrics for a specific table.
        """
        try:
            table_type, table_location, table_provider = self.get_table_properties(database_name, table_name)
            if table_type and table_location and table_provider:
                bucket_url = S3Url(table_location)
                num_files_before, total_size_before = self.get_size_and_count_files(bucket_url.bucket_name, bucket_url.prefix)
                size_human_readable_before = self.convert_size(total_size_before)

                df_detail_before = self.table_detail_before(database_name, table_name, num_files_before, total_size_before, size_human_readable_before)
                self.save_table(df_detail_before)
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

