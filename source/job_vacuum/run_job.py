import json
import time
import logging
import sys

from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from config.vacuum import VacuumJob

def configure_logging():
    """Configures the logging settings."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    # Configure logging
    configure_logging()
    
    spark = SparkSession.builder.appName("DeltaTableMaintenance").getOrCreate()
    spark.conf.set('spark.databricks.delta.vacuum.parallelDelete.enabled', 'true')

    # Assuming the same config file can be used for both optimize and vacuum jobs
    optimize_job = OptimizeJob(spark, "config/param.json")
    vacuum_job = VacuumJob(spark, "config/param.json")

    # Run optimize first
    optimize_job.run_parallel_optimize()

    # Then run vacuum
    vacuum_job.run_parallel_vacuum()
