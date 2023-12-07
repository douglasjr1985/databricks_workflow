import json
import time

from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from config.vacuum import VacuumJob


if __name__ == "__main__":
    spark = SparkSession.builder.appName("VacuumJob").getOrCreate()
    spark.conf.set('spark.databricks.delta.vacuum.parallelDelete.enabled', 'true')
    
    vacuum_job = VacuumJob(spark, "config/param.json")
    vacuum_job.run_parallel_vacuum()