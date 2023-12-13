import logging
import sys

from pyspark.sql import SparkSession
from config.optmize import OptimizeJob

def configure_logging():
    """Configures the logging settings."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    # Configure logging
    configure_logging()
    
    spark = SparkSession.builder.appName("DeltaTableMaintenance").getOrCreate()
    spark.conf.set('spark.databricks.delta.vacuum.parallelDelete.enabled', 'true')
 
    optimize_job = OptimizeJob(spark, "config/param.json")

    # Run optimize first
    optimize_job.run_parallel_optimize()