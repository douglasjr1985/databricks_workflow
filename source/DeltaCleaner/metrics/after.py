import boto3
import math
import time
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from urllib.parse import urlparse
from pyspark.sql.utils import AnalysisException

class S3Url:
    """Class to parse and handle S3 URLs."""
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket_name(self):
        """Returns the bucket name of the S3 URL."""
        return self._parsed.netloc

    @property
    def prefix(self):
        """Returns the prefix of the S3 URL."""
        return self._parsed.path.lstrip("/") if not self._parsed.query else self._parsed.path.lstrip("/") + "?" + self._parsed.query

class TableMetricsCollector:
    """Class to collect and analyze metrics of Delta tables."""
    
    def __init__(self, spark, database_name, table_name):
        self.spark = spark
        self.database_name = database_name
        self.table_name = table_name
        self.delta_table = DeltaTable.forName(spark, f'{database_name}.{table_name}')

    def get_table_properties(self):
        """Retrieve properties of the Delta table."""
        table_properties = (
            self.spark.sql(f"DESC EXTENDED {self.database_name}.{self.table_name}")
            .filter(F.col("col_name").isin("Type", "Location", "Provider"))
            .rdd.map(lambda row: (row["Type"], row["Location"], row["Provider"]))
            .collect()
        )
        if not table_properties:
            raise Exception(f"Properties not found for table {self.database_name}.{self.table_name}")
        return table_properties.pop()

    @staticmethod
    def get_size_and_count_files(bucket_name, prefix):
        """Get the size and count of files in an S3 bucket with the given prefix."""
        s3 = boto3.resource("s3")
        bucket_s3 = s3.Bucket(bucket_name)
        num_files = total_size = 0
        for b in bucket_s3.objects.filter(Prefix=prefix):
            total_size += b.size
            num_files += 1
        return num_files, total_size

    @staticmethod
    def convert_size(size_bytes):
        """Convert a size in bytes to a human-readable format."""
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        return f"{round(size_bytes / math.pow(1024, i), 2)} {size_name[i]}"

    def collect_metrics(self):
        """Collect and save metrics about the Delta table."""
        start_time = time.time()
        table_type, table_location, table_provider = self.get_table_properties()
        bucket_url = S3Url(table_location)
        num_files_before, total_size_before = self.get_size_and_count_files(bucket_url.bucket_name, bucket_url.prefix)
        size_human_readable_before = self.convert_size(total_size_before)

        # Your logic to perform operations on delta_table
        # ...

        num_files_after, total_size_after = self.get_size_and_count_files(bucket_url.bucket_name, bucket_url.prefix)
        size_human_readable_after = self.convert_size(total_size_after)

        duration = time.time() - start_time
        # Logic to log or store the metrics
        # ...

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TableMetricsCollector").getOrCreate()
    collector = TableMetricsCollector(spark, "your_database_name", "your_table_name")
    collector.collect_metrics()